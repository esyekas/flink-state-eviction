package com.github.juanrh.streaming;

import com.github.juanrh.streaming.source.ElementsWithGapsSource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
/**
 * Created by juanrh on 10/31/2016.
 *
 * Idea:
 * - try to define an example that implements a Java version of mapWithState (https://ci.apache.org/projects/flink/flink-docs-release-1.1/api/java/org/apache/flink/streaming/api/scala/KeyedStream.html#mapWithState-scala.Function2-org.apache.flink.api.common.typeinfo.TypeInformation-org.apache.flink.api.common.typeinfo.TypeInformation-)
 * with time based eviction
 * - encapsulate in an operator https://ci.apache.org/projects/flink/flink-docs-master/internals/add_operator.html
 * - add other evictions types: e.g. number of items, size, etc
 */
public class MapWithStatePoC {
    private static Logger LOG = LoggerFactory.getLogger(MapWithStatePoC.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStateBackend(new MemoryStateBackend());

//        DataStream<Tuple2<String, Integer>> inputStream =
//                env.fromElements(Tuple2.of("a", 2), Tuple2.of("b", 1), Tuple2.of("a", 3));
        // FIXME create simple test for ElementsWithGapsSource that expresses gaps
        SourceFunction<Tuple2<String, Integer>> source =
                new ElementsWithGapsSource<Tuple2<String, Integer>>()
                        .addElem(Tuple2.of("a", 2)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("b", 1)).addGap(Time.seconds(1))
                        .addElem(Tuple2.of("a", 2)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("c", 5)).addGap(Time.milliseconds(700))
                        .addElem(Tuple2.of("h", 3));

        DataStream<Tuple2<String, Integer>> inputStream =
//                env.addSource(source, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() { }));
                env.addSource(source);

        inputStream.print();

        env.execute("MapWithStatePoC");



    }
}
