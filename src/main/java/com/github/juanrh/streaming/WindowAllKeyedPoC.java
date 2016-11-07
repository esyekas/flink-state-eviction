package com.github.juanrh.streaming;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
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

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());

        DataStream<Tuple2<String, Integer>> input =
                env.fromElements(
                        Tuple2.of("a", 42), Tuple2.of("j", 52), Tuple2.of("c", 12),
                        Tuple2.of("g", 3), Tuple2.of("a", 22), Tuple2.of("h", 21),
                        Tuple2.of("a", 42), Tuple2.of("j", 52), Tuple2.of("c", 12),
                        Tuple2.of("g", 3), Tuple2.of("a", 22), Tuple2.of("h", 21));

        final int countSize = 3;
        DataStream<Tuple2<String, Integer>> windowApplied =
                input.countWindowAll(countSize).apply(new WindowAgg());
        windowApplied.print();

        final int parallelism = env.getParallelism();
        DataStream<Tuple2<String, Integer>> windowAppliedPar = input
            .keyBy(new KeySelector<Tuple2<String,Integer>, Integer>() {
                @Override
                public Integer getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    return stringIntegerTuple2.hashCode() % parallelism;
                }
            }).countWindow(countSize).apply(new WindowFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Integer, GlobalWindow>() {
                final AllWindowFunction<Tuple2<String,Integer>, Tuple2<String, Integer>, GlobalWindow> fun =
                        new WindowAgg<>();

                @Override
                public void apply(Integer integer, GlobalWindow globalWindow,
                                  Iterable<Tuple2<String, Integer>> iterable,
                                  Collector<Tuple2<String, Integer>> collector) throws Exception {
                    fun.apply(globalWindow, iterable, collector);
                }
            });
        windowAppliedPar.print();

        env.execute("WindowAllKeyedPoC");
    }
}
