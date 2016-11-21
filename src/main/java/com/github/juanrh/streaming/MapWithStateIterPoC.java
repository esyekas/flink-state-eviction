package com.github.juanrh.streaming;

import com.github.juanrh.streaming.source.ElementsWithGapsSource;
import lombok.Data;
import lombok.NonNull;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Created by juanrh on 11/13/2016.
 *
 * https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/datastream_api.html#iterations
 */
public class MapWithStateIterPoC {
    private static final Logger LOG = LoggerFactory.getLogger(MapWithStateIterPoC.class);

    public static void main(String [] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(8);
                        //.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());
        env.getConfig().disableSysoutLogging();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        SourceFunction<Tuple2<String, Integer>> source =
                ElementsWithGapsSource
                        .addElem(Tuple2.of("a", 2)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("a", 3)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("b", 1)).addGap(Time.seconds(500))
                        .addElem(Tuple2.of("c", 5)).addElem(Tuple2.of("d", 2)).addGap(Time.seconds(1))
                        .addElem(Tuple2.of("a", 3)).build();

        DataStream<Tuple2<String, Integer>> input = env.addSource(source);

        final Time ttl = Time.milliseconds(1500); // how long a state key will last (approx)
        final Time ttlRefreshInterval = Time.milliseconds(450); // how often access to each state key will be checked (approx)
        // to avoid non serializable exception in main rich function
        final long ttlMillis = ttl.toMilliseconds();
        final long ttlRefreshIntervalMillis = ttlRefreshInterval.toMilliseconds();

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = input.keyBy(0);
        DataStream<Either<Tuple2<String, Integer>, String>> eitherInputOrTombstone =
                keyed.map(new MapFunction<Tuple2<String,Integer>, Either<Tuple2<String, Integer>, String>>() {
                    @Override
                    public Either<Tuple2<String, Integer>, String> map(Tuple2<String, Integer> stringInt) throws Exception {
                        return Either.Left(stringInt);
                    }
                });
//        eitherInputOrTombstone.print();
        IterativeStream<Either<Tuple2<String,Integer>, String>> eitherInputOrTombstoneIter =
                eitherInputOrTombstone.iterate(Time.seconds(5).toMilliseconds());

        DataStream<Either<Tuple2<String,Integer>, String>> trans = //keyedEithers
          eitherInputOrTombstoneIter
                  // FIXME required?
          .keyBy(new KeySelector<Either<Tuple2<String,Integer>,String>, String>() {
              @Override
              public String getKey(Either<Tuple2<String, Integer>, String> either) throws Exception {
                  if (either.isLeft()) {
                      return either.left().f0;
                  }
                  return either.right();
              }
          })
          .flatMap(new RichFlatMapFunction<Either<Tuple2<String,Integer>, String>, Either<Tuple2<String,Integer>, String>>() {
            private final TimeStampedValue<Integer> defaultState = new TimeStampedValue(0);
//            private final long ttlMillis = ttl.toMilliseconds();
//            private final long ttlRefreshIntervalMillis = ttlRefreshInterval.toMilliseconds();
            private transient ScheduledExecutorService executor;
            private transient ValueState<TimeStampedValue<Integer>> valueState;
            @Override
            public void open(Configuration config) {
                ValueStateDescriptor<TimeStampedValue<Integer>> descriptor =
                        new ValueStateDescriptor<>(
                                "sumWithTimestamp", // the state name
                                TypeInformation.of(new TypeHint<TimeStampedValue<Integer>>() {}),
                                defaultState); // default value of the state
                valueState = getRuntimeContext().getState(descriptor);
                executor = Executors.newSingleThreadScheduledExecutor();
            }

            private void sendTombstone(final Collector<Either<Tuple2<String,Integer>, String>> collector, final String key) {
                LOG.warn("Scheduling the shipment of a tombstone for key {}", key);
                executor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        LOG.warn("Sending a tombstone for key {}", key);
                        Either<Tuple2<String,Integer>, String> tombstone = Either.Right(key);
                        collector.collect(tombstone);
                    }
                }, ttlRefreshIntervalMillis, TimeUnit.MILLISECONDS);
            }

            @Override
            public void flatMap(Either<Tuple2<String,Integer>, String> either, Collector<Either<Tuple2<String,Integer>, String>> collector) throws Exception {
                TimeStampedValue<Integer> state;
                try {
                   state = valueState.value();
                } catch (Exception e) {
                    // this was evicted, reset to defaultState
                    LOG.warn("State was evicted for key {}, reset to default state", either.left().f0);
                    state = defaultState;
                }
                if (either.isLeft()) {
                    Tuple2<String, Integer> stringInt = either.left();
                    final int currentSum = stringInt.f1 + state.getValue();
                    state.setValue(currentSum); // state business logic
                    state.setLastAccessTimestamp(System.currentTimeMillis()); // update state for the current key was last touched now
                    if (! state.isTombstoneSent()) {
                        // sent tombstone to the same key: this is only required to send the first tombstone
                            // FIXME: this only works if we have access to the key!!!
                        state.setTombstoneSent(true);
                        sendTombstone(collector, stringInt.f0);
                    }
                    valueState.update(state);
                    Either<Tuple2<String,Integer>, String> result = Either.Left(Tuple2.of(stringInt.f0, currentSum));
                    collector.collect(result);
                } else
                {
                    // we just received a tombstone
                    LOG.warn("Received a tombstone for key {}", either.right());
                    long currentTimeMillis = System.currentTimeMillis();
                    if (currentTimeMillis - state.getLastAccessTimestamp() >= ttlMillis) {
                        // evict
                        // next time the state it's used it will take the default value,
                        // that makes isTombstoneSent = false
                        valueState.clear();
                        LOG.warn("Evicted state for key " + either.right());
                    } else {
                        // send another tombstone: the current tombstone prevented sending
                        // more tombstone for events after the one that sent the current tombstone
                        sendTombstone(collector, either.right());
                    }
                }

                /*
                * Haz POJO @Data para estado y guarda isTombstoneSent para no mandar siempre el
                * Left con tombstone keyed igual y solo si se ha mandado y no recibido aun: actualiza cuando
                * recibido etc
                * */
            }
        });

        eitherInputOrTombstoneIter.closeWith(trans.filter(new FilterFunction<Either<Tuple2<String, Integer>, String>>() {
            @Override
            public boolean filter(Either<Tuple2<String, Integer>, String> either) throws Exception {
                return either.isRight();
            }
        }));
        DataStream<Tuple2<String, Integer>> out = trans.flatMap(new FlatMapFunction<Either<Tuple2<String, Integer>, String>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Either<Tuple2<String, Integer>, String> tuple2StringEither, Collector<Tuple2<String, Integer>> collector) throws Exception {
                if (tuple2StringEither.isLeft()) {
                    collector.collect(tuple2StringEither.left());
                }
            }
        });

        StreamingUtils.printWithName(eitherInputOrTombstoneIter, "eitherInputOrTombstoneIter");
        StreamingUtils.printWithName(trans, "trans");
        StreamingUtils.printWithName(out, "out");

//        //        // TODO use window for periodic launch? Or just the rights from out, and out the lefts
//        DataStream<Tuple2<String,Integer>> out = trans.flatMap(new FlatMapFunction<Either<Tuple2<String, Integer>, String>, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(Either<Tuple2<String, Integer>, String> either, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                if (either.isLeft()) {
//                    collector.collect(either.left());
//                }
//            }
//        });

        // FIXME: think test case: adapt the code to not finding a value (might require try)
            // to use a init value instead, so "a" gets a eviceted before the final "a", and we
            // gets a 3 instead of the accumulated sum of 7 in the final value

//        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = input.keyBy(0); // this fails with a serialization error for the key
//        // rights are just the key, because they just trigger checking the key
//        DataStream<Either<Tuple2<String, Integer>, String>> eithers =
//                //keyed
//                input.map(new MapFunction<Tuple2<String,Integer>, Either<Tuple2<String, Integer>, String>>() {
//                    @Override
//                    public Either<Tuple2<String, Integer>, String> map(Tuple2<String, Integer> stringInt) throws Exception {
//                        return Either.Left(stringInt);
//                    }
//                });
//
////        IterativeStream<Option<Tuple2<String, Integer>>> ttlIter = options.iterate();
//
//        // FIXME input should be options
//        KeyedStream<Either<Tuple2<String,Integer>, String>, String> keyedEithers = eithers
//                .keyBy(new KeySelector<Either<Tuple2<String,Integer>, String>, String>() {
//                    @Override
//                    public String getKey(Either<Tuple2<String, Integer>, String> either) throws Exception {
//                        if (either.isLeft()) {
//                            return either.left().f0;
//                        }
//                        return either.right();
//                    }
//                });
//
//        IterativeStream<Either<Tuple2<String,Integer>, String>> eithersIter =
//                eithers.iterate(10*1000L);
//                // eithers.iterate();
//
//        DataStream<Either<Tuple2<String,Integer>, String>> trans = //keyedEithers
//          eithersIter
//                  // FIXME: should key only once!
//          .keyBy(new KeySelector<Either<Tuple2<String,Integer>,String>, String>() {
//              @Override
//              public String getKey(Either<Tuple2<String, Integer>, String> either) throws Exception {
//                  if (either.isLeft()) {
//                      return either.left().f0;
//                  }
//                  return either.right();
//              }
//          })
//
//          .flatMap(new RichFlatMapFunction<Either<Tuple2<String,Integer>, String>, Either<Tuple2<String,Integer>, String>>() {
//            private final TimeStampedValue<Integer> defaultState = new TimeStampedValue(0);
//            private final long ttlMillis = ttl.toMilliseconds();
//            private final Logger LOG = LoggerFactory.getLogger(MapWithStateIterPoC.class);
//
//            private transient ValueState<TimeStampedValue<Integer>> valueState;
//            @Override
//            public void open(Configuration config) {
//                ValueStateDescriptor<TimeStampedValue<Integer>> descriptor =
//                        new ValueStateDescriptor<>(
//                                "sumWithTimestamp", // the state name
//                                TypeInformation.of(new TypeHint<TimeStampedValue<Integer>>() {}),
//                                defaultState); // default value of the state
//                valueState = getRuntimeContext().getState(descriptor);
//            }
//
//            @Override
//            public void flatMap(Either<Tuple2<String,Integer>, String> either, Collector<Either<Tuple2<String,Integer>, String>> collector) throws Exception {
////                TimeStampedValue<Integer> state = valueState.value();
//                TimeStampedValue<Integer> state;
////                try {
//                    state = valueState.value();
////                } catch (Exception e) {
////                    // this was evicted, reset to defaultState
////                    LOG.warn("State was evicted");
////                    state = defaultState;
////                }
//                if (either.isLeft()) {
//                    Tuple2<String, Integer> stringInt  = either.left();
//                    final int currentSum = stringInt.f1 + state.getValue();
//                    state.setValue(currentSum); // state business logic
//                    state.setLastAccessTimestamp(System.currentTimeMillis()); // update state for the current key was last touched now
//                    if (! state.isTombstoneSent()) {
//                        // sent tombstone to the same key
//                            // FIXME: this only works if we have access to the key!!!
//                        Either<Tuple2<String,Integer>, String> tombstone = Either.Right(stringInt.f0);
//                        collector.collect(tombstone);
//                        // sent it once
//                        state.setTombstoneSent(true);
//                    }
//                    valueState.update(state);
//                    Either<Tuple2<String,Integer>, String> result = Either.Left(Tuple2.of(stringInt.f0, currentSum));
//                    collector.collect(result);
//                } else
//                {
//                    // we just received a tombstone
//                    long currentTimeMillis = System.currentTimeMillis();
//                    if (currentTimeMillis - state.getLastAccessTimestamp() >= ttlMillis) {
//                        // evict
//                        LOG.warn("Evicting state for key " + either.right());
//                        valueState.clear();
//                    } else {
//                        // send another tombstone: the current tombstone prevented sending
//                        // more tombstone for events after the one that sent the current tombstone
//                        // FIXME: refactor to common place
//                        Either<Tuple2<String,Integer>, String> tombstone = either;
//                        // FIXME collector.collect(tombstone);
//                        // sent it once
//                        state.setTombstoneSent(true);
//                        valueState.update(state);
//                    }
//                }
//
//                /*
//                * Haz POJO @Data para estado y guarda isTombstoneSent para no mandar siempre el
//                * Left con tombstone keyed igual y solo si se ha mandado y no recibido aun: actualiza cuando
//                * recibido etc
//                * */
//            }
//        });
//
//        // TODO use window for periodic launch? Or just the rights from out, and out the lefts
//        DataStream<Tuple2<String,Integer>> out = trans.flatMap(new FlatMapFunction<Either<Tuple2<String, Integer>, String>, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(Either<Tuple2<String, Integer>, String> either, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                if (either.isLeft()) {
//                    collector.collect(either.left());
//                }
//            }
//        });
//
//        // FIXME: delay this event for some seconds
//        eithersIter.closeWith(trans.filter(new FilterFunction<Either<Tuple2<String, Integer>, String>>() {
//            @Override
//            public boolean filter(Either<Tuple2<String, Integer>, String> either) throws Exception {
//                return either.isRight();
//            }
//        }));
////         // FIXME not sure if this is required: maybe filter respects keys
////         .keyBy(new KeySelector<Either<Tuple2<String,Integer>,String>, String>() {
////            @Override
////            public String getKey(Either<Tuple2<String, Integer>, String> either) throws Exception {
////                return either.right();
////            }
////        });
//
//        // FIXME: this never ends, use that iterate argument for ending
//
////        trans.print();
//        out.print();


        env.execute("MapWithStateIterPoC");
    }

    @Data
    public static class TimeStampedValue<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        @NonNull private T value;
        /** millis since UNIX epoch like in System.currentTimeMillis */
        private long lastAccessTimestamp;
        private boolean isTombstoneSent;

        public TimeStampedValue(@NonNull T value) {
            this.value = value;
            this.lastAccessTimestamp = 0;
            this.isTombstoneSent = false;
        }
    }
}

