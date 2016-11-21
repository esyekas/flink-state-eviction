package com.github.juanrh.streaming;

import com.github.juanrh.streaming.source.ElementsWithGapsSource;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.experimental.Delegate;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.Option;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by juanrh on 10/31/2016.
 *
 * Idea:
 * - try to define an example that implements a Java version of mapWithState (https://ci.apache.org/projects/flink/flink-docs-release-1.1/api/java/org/apache/flink/streaming/api/scala/KeyedStream.html#mapWithState-scala.Function2-org.apache.flink.api.common.typeinfo.TypeInformation-org.apache.flink.api.common.typeinfo.TypeInformation-)
 * with time based eviction
 * - encapsulate in an operator https://ci.apache.org/projects/flink/flink-docs-master/internals/add_operator.html
 * - add other evictions types: e.g. number of items, size, etc
 */
// FIXME checkpointable: if not enoufh whtat it is in the state, that I guess it's checkpointed
public class MapWithStatePoC {
    private static Logger LOG = LoggerFactory.getLogger(MapWithStatePoC.class);

    // FIXME is this already partioning the state when used with KeyedStream
    // as described in the doc for the original mapWithState?
    // public <R,S> DataStream<R> mapWithState(scala.Function2<T,scala.Option<S>,scala.Tuple2<R,scala.Option<S>>> fun,

    public abstract static class MapWithStateWithEviction<IN, OUT, STATE> extends RichMapFunction<IN, OUT> {
        private static Logger LOG = LoggerFactory.getLogger(MapWithStateWithEviction.class);

//        private transient Function<Tuple2<IN, Optional<STATE>>, Tuple2<OUT, Optional<STATE>> mapFunction;
        // IDEA: as keys are not explicit, store last time used in state
        private transient ValueState<Tuple2<Long, STATE>> state;

        private transient Optional<ScheduledExecutorService> scheduler = Optional.absent();
        private transient Optional<Time> expirationTime = Optional.absent();

//        @AllArgsConstructor
//        public abstract static class FIXNameOptional<T> extends Optional<T> {
//            private interface Get<V> {
//                V get();
//            }
//            @Delegate(excludes=Get.class)
//            private Optional<T> self;
//            private ValueState<Tuple2<Long, T>> state;
//
//            @Override
//            public T get() {
////                Tuple2<Long, T> newState = state.value();
////                newState.setField(System.currentTimeMillis(), 0);
////                state.update(newState);
//                return self.get(); // This doesn't make sense: why the complication
//                // --> instead extend state to add this stuff, and also ask the user
//                // for a defaul value for state. But first how am I doing eviction?, how
//                // I get all the keys and their values?, how pass from executor to its keys?
//            }
//        }

        // FIXME: look for functions with 2 args or define new interface
        public MapWithStateWithEviction(@NonNull Function<Tuple2<IN, Optional<STATE>>, Tuple2<OUT, Optional<STATE>>> mapFunction) {
          //this.mapFunction = mapFunction;
        }

        public MapWithStateWithEviction<IN, OUT, STATE> withExpiration(Time expirationTime) {
            this.expirationTime = Optional.of(expirationTime);
            return this;
        }

        @Override
        public void open(Configuration config) {
//            ValueStateDescriptor<Tuple2<Long, STATE>> stateDescriptor =
//                    new ValueStateDescriptor<>(
//                            "FIXME", // FIXME: optional name with fluent setter, or default name with UUID
//                            TypeInformation.of(new TypeHint<Tuple2<Long, STATE>>(){}),
//                            Tuple2.of(0,null)); // NOTE: read with Optional
//            state = getRuntimeContext().getState(stateDescriptor);
//            if (expirationTime.isPresent()) {
//                scheduler = Optional.of(Executors.newSingleThreadScheduledExecutor());
//                scheduler.get().scheduleAtFixedRate(new Runnable() {
//                    @Override
//                    public void run() {
//                        // FIXME
//
//                    }
//                }, expirationTime.get().toMilliseconds(), expirationTime.get().toMilliseconds(), TimeUnit.MILLISECONDS);
//            }
        }

        @Override
        public void close() {
            if (scheduler.isPresent()) {
                try {
                    scheduler.get().shutdownNow();
                } catch (Exception e) {
                    LOG.warn("Error shutting down state eviction scheduler" , e);
                }
            }
        }

        @Override
        public OUT map(IN in) throws Exception {
            System.currentTimeMillis();

            return null;
        }
    }


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                //StreamExecutionEnvironment.createLocalEnvironment();
        env.setStateBackend(new MemoryStateBackend());

        // FIXME create simple test for ElementsWithGapsSource that expresses gaps
        SourceFunction<Tuple2<String, Integer>> source =
                ElementsWithGapsSource
                        .addElem(Tuple2.of("a", 2)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("b", 1)).addGap(Time.seconds(1))
                        .addElem(Tuple2.of("a", 2)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("c", 5)).addElem(Tuple2.of("d", 2)).addGap(Time.seconds(1))
                        .addElem(Tuple2.of("h", 3)).build();

        DataStream<Tuple2<String, Integer>> inputStream = env.addSource(source);

        inputStream.print();

        JobExecutionResult jobResult = env.execute("MapWithStatePoC");
        System.out.println("Job completed in " + jobResult.getNetRuntime() + " milliseconds" );
    }
}
