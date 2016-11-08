package com.github.juanrh.streaming.windowAllPoCs;

import com.github.juanrh.streaming.source.ElementsWithGapsSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/*
* FIXME: redo for reduce function that is the only that makes sense
* */

/**
 * Created by juanrh on 11/6/2016.
 */
public class WindowAllTimeKeyedPoC {
    /*
    * Analogous to KeyedStream<T,KEY> {  public <W extends Window> WindowedStream<T,KEY,W> window(WindowAssigner<? super T,W> assigner) }
    * but for DataStream<T>
    * */
    public static <T, W extends Window> WindowedStream<T, Integer, W> window(final DataStream<T> stream,
                                                                             final int parallelism,
                                                                             WindowAssigner<? super T,W> assigner) {
        return stream
                .keyBy(new KeySelector<T, Integer>() {
                    @Override
                    public Integer getKey(T value) throws Exception {
                        return value.hashCode() % parallelism;
                    }
                })
                .window(assigner);
    }

    /**
     * Tries to create a parallel version of a AllWindowStream for a DataStream
     * by creating a KeyedStream by using as key the hash of the elements module
     * a parallelism level
     *
     * This only make sense for window assigners that ensure the subwindows will be
     * in sync, like time based window assigners. This doesn't work for counting
     * or sessions window assigners
     * */
    public static class ParAllWindowedStream<T,W extends Window> extends WindowedStream<T, Integer, W> {
        private final transient WindowAssigner<? super T,W> windowAssigner;

        public ParAllWindowedStream(DataStream<T> stream, final int parallelism,
                                    WindowAssigner<? super T,W> windowAssigner) {
            super(stream.keyBy(new KeySelector<T, Integer>() {
                               @Override
                                public Integer getKey(T value) throws Exception {
                                    return value.hashCode() % parallelism;
                                }
                            }),
                  windowAssigner);
            this.windowAssigner = windowAssigner;
        }

        @Override
        public SingleOutputStreamOperator<T> reduce(ReduceFunction<T> reduceFun) {
            return super.reduce(reduceFun)      // reduce each subwindow
                    .windowAll(windowAssigner)  // synchronize
                    .reduce(reduceFun);         // sequential aggregate of
        }
    }


    /*
    *
1> allWindowedReduced: (a-b,3)
2> allWindowedReduced: (a-c,7)
1> parAllWindowedApplied: (a-b,3)
3> allWindowedReduced: (b-c,12)
2> parAllWindowedApplied: (a-c,7)
4> allWindowedReduced: (a-c,17)     <-- missing last element for par if the last gap is too tight: in this example last gap of 500
3> parAllWindowedApplied: (c-b,12)  <-- out of order for par
    * */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());
        env.getConfig().disableSysoutLogging();

        SourceFunction<Tuple2<String, Integer>> source =
                ElementsWithGapsSource
                        .addElem(Tuple2.of("a", 1)).addGap(Time.milliseconds(20)).addElem(Tuple2.of("b", 2)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("a", 3)).addElem(Tuple2.of("c", 4)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("b", 5)).addElem(Tuple2.of("c", 7)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("a", 8)).addElem(Tuple2.of("c", 9)).addGap(Time.milliseconds(800)).build();
        DataStream<Tuple2<String, Integer>> input = env.addSource(source);

        org.apache.flink.streaming.api.windowing.time.Time windowSize = org.apache.flink.streaming.api.windowing.time.Time.milliseconds(500);
        WindowAssigner<Object, TimeWindow> windowAssigner = TumblingProcessingTimeWindows.of(windowSize);

        final ReduceFunction<Tuple2<String, Integer>> redFun = new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc, Tuple2<String, Integer> v) {
                acc.setField(acc.f0 + "-" + v.f0, 0);
                acc.setField(acc.f1 + v.f1, 1);
                return acc;
            }
        };

        AllWindowedStream<Tuple2<String, Integer>, TimeWindow> allWindowed = input.windowAll(windowAssigner);
        DataStream<Tuple2<String, Integer>> allWindowedApplied = allWindowed.reduce(redFun);
        WindowAllKeyedPoC.printWithName(allWindowedApplied, "allWindowedReduced");

        // Parallel attempt
        final int parallelism = 2; //env.getParallelism();

        ParAllWindowedStream<Tuple2<String, Integer>, TimeWindow> parAllWindowed =
                new ParAllWindowedStream(input, parallelism, windowAssigner);
        DataStream<Tuple2<String, Integer>> parAllWindowedApplied = parAllWindowed.reduce(redFun);
        WindowAllKeyedPoC.printWithName(parAllWindowedApplied, "parAllWindowedApplied");

                // TODO print keyed version to see the 1 window per paralelism

        // TODO print parallel version of allWindow that should print just 1 window in total

        env.execute();
    }
}
