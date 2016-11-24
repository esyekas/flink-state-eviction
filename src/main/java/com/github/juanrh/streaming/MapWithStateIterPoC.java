package com.github.juanrh.streaming;

import com.github.juanrh.streaming.source.ElementsWithGapsSource;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
/*
16/11/23 22:07:59 WARN InstanceConnectionInfo: No hostname could be resolved for the IP address 127.0.0.1, using IP address as host name. Local input split assignment (such as for HDFS files) may be impacted.
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/23 22:07:59 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
1> input: (a,2)
2> eitherInputOrTombstoneIter: Left((a,2))
2> out: (a,2)
2> trans: Left((a,2))
16/11/23 22:07:59 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key a
2> input: (a,3)
2> out: (a,5)
2> trans: Left((a,5))
2> eitherInputOrTombstoneIter: Left((a,3))
16/11/23 22:08:00 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key a
2> trans: Right(a)
16/11/23 22:08:00 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key a
16/11/23 22:08:00 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key a
2> eitherInputOrTombstoneIter: Right(a)
7> eitherInputOrTombstoneIter: Left((b,1))
16/11/23 22:08:00 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key b
7> out: (b,1)
7> trans: Left((b,1))
3> input: (b,1)
2> trans: Right(a)
16/11/23 22:08:00 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key a
16/11/23 22:08:00 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key a
16/11/23 22:08:00 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key a
2> eitherInputOrTombstoneIter: Right(a)
7> trans: Right(b)
16/11/23 22:08:00 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key b
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key c
7> eitherInputOrTombstoneIter: Left((c,5))
7> out: (c,5)
7> trans: Left((c,5))
1> out: (d,2)
7> eitherInputOrTombstoneIter: Right(b)
1> trans: Left((d,2))
1> eitherInputOrTombstoneIter: Left((d,2))
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key d
5> input: (d,2)
4> input: (c,5)
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key b
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key b
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key a
2> trans: Right(a)
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key a
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Evicted state for key a
2> eitherInputOrTombstoneIter: Right(a)
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key c
7> trans: Right(c)
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key d
1> trans: Right(d)
7> trans: Right(b)
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key b
7> eitherInputOrTombstoneIter: Right(c)
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key c
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key c
7> eitherInputOrTombstoneIter: Right(b)
1> eitherInputOrTombstoneIter: Right(d)
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key d
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key d
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key b
16/11/23 22:08:01 WARN MapWithStateIterPoC$CoreTransformationFunction: Evicted state for key b
2> out: (a,3)
2> trans: Left((a,3))
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key a
7> out: (c,7)
7> trans: Left((c,7))
7> eitherInputOrTombstoneIter: Left((c,2))
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key c
7> trans: Right(c)
7> input: (c,2)
6> input: (a,3)
2> eitherInputOrTombstoneIter: Left((a,3))
1> trans: Right(d)
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key d
7> eitherInputOrTombstoneIter: Right(c)
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key c
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key c
1> eitherInputOrTombstoneIter: Right(d)
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key d
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Evicted state for key d
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key a
2> trans: Right(a)
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key a
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key a
2> eitherInputOrTombstoneIter: Right(a)
7> trans: Right(c)
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key c
7> eitherInputOrTombstoneIter: Right(c)
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key c
16/11/23 22:08:02 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key c
2> trans: Right(a)
16/11/23 22:08:03 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key a
16/11/23 22:08:03 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key a
16/11/23 22:08:03 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key a
2> eitherInputOrTombstoneIter: Right(a)
7> trans: Right(c)
16/11/23 22:08:03 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key c
7> eitherInputOrTombstoneIter: Right(c)
16/11/23 22:08:03 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key c
16/11/23 22:08:03 WARN MapWithStateIterPoC$CoreTransformationFunction: Evicted state for key c
16/11/23 22:08:03 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key a
2> trans: Right(a)
16/11/23 22:08:03 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key a
16/11/23 22:08:03 WARN MapWithStateIterPoC$CoreTransformationFunction: Evicted state for key a
2> eitherInputOrTombstoneIter: Right(a)
8> input: (c,1)
16/11/23 22:08:04 WARN ElementsWithGapsSource: Source stopped
16/11/23 22:08:04 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key c
7> eitherInputOrTombstoneIter: Left((c,1))
7> out: (c,1)
7> trans: Left((c,1))
16/11/23 22:08:04 WARN ExecutionGraph: Received accumulator result for unknown execution 15478db2b0dc56ff2f843e33bdee6215.
16/11/23 22:08:04 WARN ExecutionGraph: Received accumulator result for unknown execution 7a4a3468da0cc8d8acd4aab6589d86a8.
7> trans: Right(c)
16/11/23 22:08:04 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key c
7> eitherInputOrTombstoneIter: Right(c)
16/11/23 22:08:04 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key c
16/11/23 22:08:04 WARN MapWithStateIterPoC$CoreTransformationFunction: Scheduling the shipment of a tombstone for key c
7> trans: Right(c)
16/11/23 22:08:05 WARN MapWithStateIterPoC$CoreTransformationFunction: Sending a tombstone for key c
7> eitherInputOrTombstoneIter: Right(c)
16/11/23 22:08:05 WARN MapWithStateIterPoC$CoreTransformationFunction: Received a tombstone for key c
16/11/23 22:08:05 WARN MapWithStateIterPoC$CoreTransformationFunction: Evicted state for key c
16/11/23 22:08:10 WARN MapWithStateIterPoC$CoreTransformationFunction: Closed function
16/11/23 22:08:10 WARN MapWithStateIterPoC$CoreTransformationFunction: Closed function
16/11/23 22:08:10 WARN MapWithStateIterPoC$CoreTransformationFunction: Closed function
16/11/23 22:08:10 WARN MapWithStateIterPoC$CoreTransformationFunction: Closed function
16/11/23 22:08:10 WARN MapWithStateIterPoC$CoreTransformationFunction: Closed function
16/11/23 22:08:10 WARN MapWithStateIterPoC$CoreTransformationFunction: Closed function
16/11/23 22:08:10 WARN MapWithStateIterPoC$CoreTransformationFunction: Closed function
16/11/23 22:08:10 WARN MapWithStateIterPoC$CoreTransformationFunction: Closed function

Process finished with exit code 0
* */
public class MapWithStateIterPoC {
    private static final Logger LOG = LoggerFactory.getLogger(MapWithStateIterPoC.class);

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

    @RequiredArgsConstructor
    public static class CoreTransformationFunction
        extends RichFlatMapFunction<Either<Tuple2<String,Integer>, String>, Either<Tuple2<String,Integer>, String>>
        implements Checkpointed<LinkedList<String>>  {

        private final long ttlMillis;
        private final long ttlRefreshIntervalMillis;

        private static final Logger LOG = LoggerFactory.getLogger(CoreTransformationFunction.class);
        private static final TimeStampedValue<Integer> DEFAULT_STATE = new TimeStampedValue(0);

        // FIXME consider replacing by Akka scheduler http://doc.akka.io/docs/akka/2.4.4/java/scheduler.html
        // if the ActorContext is available somehow
        private transient ScheduledExecutorService executor;
        private transient ValueState<TimeStampedValue<Integer>> valueState;
        private transient Set<String> pendingTombstones;
        private transient List<String> recoveringTombstones = null;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TimeStampedValue<Integer>> descriptor =
                    new ValueStateDescriptor<>(
                            "sumWithTimestamp", // the state name
                            TypeInformation.of(new TypeHint<TimeStampedValue<Integer>>() {}),
                            DEFAULT_STATE); // default value of the state
            valueState = getRuntimeContext().getState(descriptor);
            executor = Executors.newSingleThreadScheduledExecutor();
            pendingTombstones = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        }

        @Override
        public void close() {
            // just shutdown ASAP, it's ok to lose the pending
            // tombstones as the state will be deleted anyway
            executor.shutdownNow();
            LOG.warn("Closed function");
        }

        private void sendTombstone(final Collector<Either<Tuple2<String,Integer>, String>> collector, final String key) {
            LOG.warn("Scheduling the shipment of a tombstone for key {}", key);
            pendingTombstones.add(key);
            executor.schedule(new Runnable() {
                @Override
                public void run() {
                    LOG.warn("Sending a tombstone for key {}", key);
                    Either<Tuple2<String,Integer>, String> tombstone = Either.Right(key);
                    collector.collect(tombstone);
                    pendingTombstones.remove(key);
                }
            }, ttlRefreshIntervalMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public void flatMap(Either<Tuple2<String,Integer>, String> either, Collector<Either<Tuple2<String,Integer>, String>> collector) throws Exception {
            if (recoveringTombstones != null) {
                for (String tombstone : recoveringTombstones) {
                    LOG.warn("Recovering tombstone {}", tombstone);
                    sendTombstone(collector, tombstone);
                }
                recoveringTombstones = null;
            }
            final TimeStampedValue<Integer> state = valueState.value();
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
            } else {
                // we just received a tombstone
                LOG.warn("Received a tombstone for key {}", either.right());
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis - state.getLastAccessTimestamp() >= ttlMillis) {
                    // evict and stop sending tombstones for this key
                    // next time this key appears and it uses the state, the default state
                    // that makes isTombstoneSent = false will be used, and that will send
                    // the first tombstone of the new eviction cycle
                    valueState.clear();
                    LOG.warn("Evicted state for key " + either.right());
                } else {
                    // send another tombstone: the current tombstone prevented sending
                    // more tombstone for events after the one that sent the current tombstone
                    sendTombstone(collector, either.right());
                }
            }
        }

        @Override
        public LinkedList<String> snapshotState(long l, long l1) throws Exception {
            LinkedList<String> state = Lists.newLinkedList(pendingTombstones);
            state.addAll(recoveringTombstones);
            return state;
        }

        @Override
        public void restoreState(LinkedList<String> tombstones) throws Exception {
            /* Send all the pending tombstone during the recovery, which is a good enough
            approximation even though would lead to sending the tombstones a little early */
            // only have access to the collector in flatMap, keep in recoveringTombstones
            // until next call to flatMap
            recoveringTombstones = Lists.newLinkedList(tombstones);
        }
    }

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
                        .addElem(Tuple2.of("b", 1)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("c", 5)).addElem(Tuple2.of("d", 2)).addGap(Time.seconds(1))
                        .addElem(Tuple2.of("a", 3)).addElem(Tuple2.of("c", 2))
                        .addGap(Time.seconds(2)).addElem(Tuple2.of("c", 1)).build();

        DataStream<Tuple2<String, Integer>> input = env.addSource(source);

        final Time ttl = Time.milliseconds(1100); // how long a state key will last (approx)
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
        IterativeStream<Either<Tuple2<String,Integer>, String>> eitherInputOrTombstoneIter =
                eitherInputOrTombstone.iterate(Time.seconds(5).toMilliseconds());

        DataStream<Either<Tuple2<String,Integer>, String>> trans =
            eitherInputOrTombstoneIter
                    // required to avoid java.lang.RuntimeException: State key serializer has not been configured
                    // in the config. This operation cannot use partitioned state.
                    .keyBy(new KeySelector<Either<Tuple2<String,Integer>,String>, String>() {
                        @Override
                        public String getKey(Either<Tuple2<String, Integer>, String> either) throws Exception {
                            if (either.isLeft()) {
                                return either.left().f0;
                            }
                            return either.right();
                        }
                    })
                    .flatMap(new CoreTransformationFunction(ttlMillis, ttlRefreshIntervalMillis));

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
        StreamingUtils.printWithName(input, "input");
        StreamingUtils.printWithName(eitherInputOrTombstoneIter, "eitherInputOrTombstoneIter");
        StreamingUtils.printWithName(trans, "trans");
        StreamingUtils.printWithName(out, "out");

        env.execute("MapWithStateIterPoC");
    }
}

