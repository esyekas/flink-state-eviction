package com.github.juanrh.streaming.windowAllPoCs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class WindowAllKeyedPoC {
    public static class WindowAgg<W extends Window> implements AllWindowFunction<Tuple2<String,Integer>, Tuple2<String, Integer>, W> {
        @Override
        public void apply(W window,
                          Iterable<Tuple2<String, Integer>> values,
                          Collector<Tuple2<String, Integer>> out) throws Exception {
            StringBuffer label = new StringBuffer(window.maxTimestamp() + " --> ");
            int count = 0;
            for (Tuple2<String, Integer> value : values) {
                count += value.f1;
                label.append(value.f0 + "-");
            }
            out.collect(Tuple2.of(label.substring(0, label.length()-1), count));
        }
    }

    /*
    * Analogous to KeyedStream<T,KEY> {  public WindowedStream<T,KEY,GlobalWindow> countWindow(long size) }
    * but for DataStream<T>
    * */
    public static <T> WindowedStream<T,Integer,GlobalWindow> countWindow(final DataStream<T> stream,
                                                                         final int parallelism,
                                                                         final long size) {
        return stream
                .keyBy(new KeySelector<T, Integer>() {
                    @Override
                    public Integer getKey(T value) throws Exception {
                        return value.hashCode() % parallelism;
                    }
                })
                .countWindow(size);
    }

    public static <T> void printWithName(final DataStream<T> stream, final String name) {
        stream.map(new MapFunction<T, String>() {
            @Override
            public String map(T value) throws Exception {
                return name + ": " + value ;
            }
        }).print();
    }

    /*
An execution:

7> reduced: (b,2)
7> reduced: (c,4)
7> reduced: (b,7)
7> reduced: (c,11)
7> reduced: (c,20)

2> reduced: (a,1)
2> reduced: (a,4)
2> reduced: (a,12)

1> allWindowedApplied: (9223372036854775807 --> a-b,3)
2> allWindowedApplied: (9223372036854775807 --> a-c,7)
3> allWindowedApplied: (9223372036854775807 --> b-c,12)
4> allWindowedApplied: (9223372036854775807 --> a-c,17)

7> windowedAppliedPar: (9223372036854775807 --> a-b,3)
7> windowedAppliedPar2: (9223372036854775807 --> a-b,3)
* */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());
        env.getConfig().disableSysoutLogging();

        DataStream<Tuple2<String, Integer>> input =
                env.fromElements(
                        Tuple2.of("a", 1), Tuple2.of("b", 2),
                        Tuple2.of("a", 3), Tuple2.of("c", 4),
                        Tuple2.of("b", 5), Tuple2.of("c", 7),
                        Tuple2.of("a", 8), Tuple2.of("c", 9));

        final int countSize = 2; //3;
        AllWindowedStream<Tuple2<String, Integer>, GlobalWindow> allWindowed = input.countWindowAll(countSize);
        DataStream<Tuple2<String, Integer>> allWindowedApplied =
                allWindowed.apply(new WindowAgg());
        //allWindowedApplied.print();
        printWithName(allWindowedApplied, "allWindowedApplied");

        System.out.println("env.getParallelism() = " + env.getParallelism());
        final int parallelism = 8; //env.getParallelism();
        /*
        * This returns different results than a AllWindow approach for a count
        * based window, because this creates an independent window per key value,
        * and
        *  1. Each window is completed independently: so with parallelism = 8
        *  we have 8 windows and we need 2*8 = 16 values in the best case (values
        *  have evenly distributed hashes). Also note windows don't emit a result
        *  until completed, so windows that are not completed before the program
        *  ends are discarded
        *  2. produces a different aggregation per window
        *
        * For time based windows this might have more sense because:
        * 1. All time windows complete at the same time
        * 2. We might aggregate the result of the time window with the same AllWindowFunction: but
        * for that we need that the result of the aggregation is the same as the input ==> ok just
        * for Reduce functions
        * */
        WindowedStream<Tuple2<String, Integer>, Integer, GlobalWindow> windowed = input
           .keyBy(new KeySelector<Tuple2<String,Integer>, Integer>() {
                @Override
                public Integer getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    return stringIntegerTuple2.hashCode() % parallelism;
                }
        }).countWindow(countSize);
        WindowFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Integer, GlobalWindow> applyFunPar =
                new WindowFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Integer, GlobalWindow>() {
                    final AllWindowFunction<Tuple2<String,Integer>, Tuple2<String, Integer>, GlobalWindow> fun =
                            new WindowAgg<>();

                    @Override
                    public void apply(Integer key, GlobalWindow globalWindow,
                                      Iterable<Tuple2<String, Integer>> values,
                                      Collector<Tuple2<String, Integer>> out) throws Exception {
                        fun.apply(globalWindow, values, out);
                    }
                };
        DataStream<Tuple2<String, Integer>> windowedAppliedPar =
            windowed.apply(applyFunPar);
        // windowedAppliedPar.print();
        printWithName(windowedAppliedPar, "windowedAppliedPar");

        /*
        * This works the same as windowedAppliedPar, as it is essentially the same
        * */
        DataStream<Tuple2<String, Integer>> windowedAppliedPar2 =
                countWindow(input, parallelism, countSize)
                .apply(applyFunPar);
        // windowedAppliedPar2.print();
        printWithName(windowedAppliedPar2, "windowedAppliedPar2");

        // this reduces  by the groups defined in keyBy, just like fold
        printWithName(input.keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
                assert v1.f0 == v2.f0; // ensured by keyBy
                return Tuple2.of(v1.f0, v1.f1 + v2.f1);
            }
        }), "reduced");

        env.execute("WindowAllKeyedPoC");
    }
}

