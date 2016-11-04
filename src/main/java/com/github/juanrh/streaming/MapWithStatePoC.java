package com.github.juanrh.streaming;

import com.github.juanrh.streaming.source.ElementsWithGapsSource;
import org.apache.flink.api.common.JobExecutionResult;
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

        DataStream<Tuple2<String, Integer>> inputStream =
                env.addSource(source);

        inputStream.print();

        JobExecutionResult jobResult = env.execute("MapWithStatePoC");
        System.out.println("Job completed in " + jobResult.getNetRuntime() + " milliseconds" );
    }
}
