package com.github.juanrh.streaming.source;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

/*
* FIXME: but this is the idea. Anyway write down the other
 * ideas I had and see if covered by this, or can contribute with other
*
It is true that there is no guide. There is
https://github.com/ottogroup/flink-spector for testing streaming
pipelines.

For unit tests and integration tests please have a look at the Flink
source code which contains many such tests.
* */

// FIXME: consider moving this test to another package: is this a test
// for the source or for the sink?, or for both?
import com.github.juanrh.streaming.sink.ElementsWithGapsSink;

import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;

/*
Following
    https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/util/StreamingMultipleProgramsTestBase.html
*/
public class ElementsWithGapsSourceTest extends StreamingMultipleProgramsTestBase {

    @Test
    public void foo() throws Exception {
        // get shared execution context
        // https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/util/StreamingMultipleProgramsTestBase.html
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                env.addSource(source)
                //.setParallelism(2) // fails with paralellism > 1 because this source it's not parallel
                ;
        DataStream<String> letters = inputStream.map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        })
                //.setParallelism(2)
                ;
        ElementsWithGapsSink<String> expectedSink =
              //  new ElementsWithGapsSink<>(Lists.newArrayList("a", "b", "a", "d", "c", "h"));
                // this makes the test fail:
                //new ElementsWithGapsSink<>(Lists.newArrayList("a")); // TODO put in failing test case
              new ElementsWithGapsSink<>(Lists.newArrayList("a", "a", "a", "d", "c", "h"))// TODO put in failing test case
            ;
        letters
                //.broadcast()
                .addSink(expectedSink);
                //.setParallelism(1); // This is not a method of DataStream!

        inputStream.print();

        env.execute("MapWithStatePoC");

        // This is very little expressive, collecting the result and comparing
        // would be ritcher, but when we implement the harmcrest matcher we
        // can fail after the first failing prefix and give a better message here
        // (maybe also keep original collection)
        assertFalse(expectedSink.isMatchingFailure());
        System.out.println("isMatchingFailure = " + expectedSink.isMatchingFailure());
    }

    @Ignore("TODO")
    @Test
    public void bar() throws Exception {
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

        env.execute("MapWithStatePoC");
    }
}
