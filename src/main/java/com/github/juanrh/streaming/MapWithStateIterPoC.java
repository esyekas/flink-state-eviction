package com.github.juanrh.streaming;

import com.github.juanrh.streaming.source.ElementsWithGapsSource;
import lombok.Data;
import lombok.NonNull;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
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
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Created by juanrh on 11/13/2016.
 *
 * https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/datastream_api.html#iterations
 */
public class MapWithStateIterPoC {

    public static void main(String [] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());
        env.getConfig().disableSysoutLogging();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime); // activate ingestion time

        // FIXME create simple test for ElementsWithGapsSource that expresses gaps
        SourceFunction<Tuple2<String, Integer>> source =
                ElementsWithGapsSource
                        .addElem(Tuple2.of("a", 2)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("b", 1)).addGap(Time.seconds(1))
                        .addElem(Tuple2.of("a", 2)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("c", 5)).addElem(Tuple2.of("d", 2)).addGap(Time.seconds(1))
                        .addElem(Tuple2.of("a", 3)).build();
        DataStream<Tuple2<String, Integer>> input = env.addSource(source);
        final Time ttl = Time.milliseconds(750);
            // FIXME: think test case: adapt the code to not finding a value (might require try)
            // to use a init value instead, so "a" gets a eviceted before the final "a", and we
            // gets a 3 instead of the accumulated sum of 7 in the final value

//        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = input.keyBy(0); // this fails with a serialization error for the key
        // rights are just the key, because they just trigger checking the key
        DataStream<Either<Tuple2<String, Integer>, String>> eithers =
                //keyed
                input.map(new MapFunction<Tuple2<String,Integer>, Either<Tuple2<String, Integer>, String>>() {
                    @Override
                    public Either<Tuple2<String, Integer>, String> map(Tuple2<String, Integer> stringInt) throws Exception {
                        return Either.Left(stringInt);
                    }
                });

//        IterativeStream<Option<Tuple2<String, Integer>>> ttlIter = options.iterate();

        // FIXME input should be options
        KeyedStream<Either<Tuple2<String,Integer>, String>, String> keyedEithers = eithers
                .keyBy(new KeySelector<Either<Tuple2<String,Integer>, String>, String>() {
                    @Override
                    public String getKey(Either<Tuple2<String, Integer>, String> either) throws Exception {
                        if (either.isLeft()) {
                            return either.left().f0;
                        }
                        return either.right();
                    }
                });
        DataStream<Either<Tuple2<String,Integer>,
                String>> trans = keyedEithers.flatMap(new RichFlatMapFunction<Either<Tuple2<String,Integer>, String>, Either<Tuple2<String,Integer>, String>>() {
            private final TimeStampedValue<Integer> defaultState = new TimeStampedValue(0);
            // (sum, timestamp)
            private transient ValueState<TimeStampedValue<Integer>> sumWithTimestamp;
            @Override
            public void open(Configuration config) {
                ValueStateDescriptor<TimeStampedValue<Integer>> descriptor =
                        new ValueStateDescriptor<>(
                                "sumWithTimestamp", // the state name
                                TypeInformation.of(new TypeHint<TimeStampedValue<Integer>>() {}),
                                defaultState); // default value of the state
                sumWithTimestamp = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void flatMap(Either<Tuple2<String,Integer>, String> either, Collector<Either<Tuple2<String,Integer>, String>> collector) throws Exception {
                TimeStampedValue<Integer> state = sumWithTimestamp.value();
                if (either.isLeft()) {
                    Tuple2<String, Integer> stringInt  = either.left();
                    final int currentSum = stringInt.f1 + state.getValue();
                    state.setLastAccessTimestamp(System.currentTimeMillis()); // state for the current key was last touched now
                    sumWithTimestamp.update(state);
                    Either<Tuple2<String,Integer>, String> res = Either.Left(Tuple2.of(stringInt.f0, currentSum));
                    collector.collect(res);
                }

                /*
                * Haz POJO @Data para estado y guarda isTombstoneSent para no mandar siempre el
                * Left con tombstone y solo si se ha mandado y no recibido aun: actualiza cuando
                * recibido etc
                * */
            }
        });

        trans.print();
        env.execute();
    }

    @Data
    public static class TimeStampedValue<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        @NonNull private T value;
        @NonNull private long lastAccessTimestamp;
        @NonNull private boolean isTombstoneSent;

        public TimeStampedValue(@NonNull T value) {
            this.value = value;
            this.lastAccessTimestamp = 0;
            this.isTombstoneSent = false;
        }
    }
}

