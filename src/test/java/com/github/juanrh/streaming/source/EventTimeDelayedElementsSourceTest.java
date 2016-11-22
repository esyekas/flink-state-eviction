package com.github.juanrh.streaming.source;

import com.github.juanrh.streaming.StreamingUtils;
import com.google.common.collect.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.contrib.streaming.DataStreamUtils;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.junit.Test;

import java.util.List;

/*
Following
    https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/util/StreamingMultipleProgramsTestBase.html
*/
public class EventTimeDelayedElementsSourceTest  extends StreamingMultipleProgramsTestBase {

    public static StreamExecutionEnvironment getEnv() {
        // get shared execution context
        // https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/util/StreamingMultipleProgramsTestBase.html
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());
        env.getConfig().disableSysoutLogging();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // activate ingestion time
        return env;
    }

    @Test
    public void simpleWindow_EventTimeDelayedElementsSourceWithEqualGapsEarly_RespectsTime() throws Exception {
        simpleWindow_EventTimeDelayedElementsSourceWithEqualGaps_RespectsTime(true);
    }

    @Test
    public void simpleWindow_EventTimeDelayedElementsSourceWithEqualGapsLate_RespectsTime() throws Exception {
        simpleWindow_EventTimeDelayedElementsSourceWithEqualGaps_RespectsTime(false);
    }


    public void simpleWindow_EventTimeDelayedElementsSourceWithEqualGaps_RespectsTime(boolean earlyImplementation) throws Exception {
        final StreamExecutionEnvironment env = getEnv();

        List<Integer> inputList = ContiguousSet.create(Range.closed(1, 6), DiscreteDomain.integers()).asList();
        DataStream<Integer> input =
                EventTimeDelayedElementsSource.withEqualGaps(env, Time.milliseconds(500), inputList)
                        .setEarlyImplementation(earlyImplementation)
                        .get();

        StreamingUtils.printWithName(input, "input");

        DataStream<Integer> result =  input.timeWindowAll(Time.seconds(1))
              .reduce(new ReduceFunction<Integer>() {
                  @Override
                  public Integer reduce(Integer i1, Integer i2) throws Exception {
                      return i1 + i2;
                  }
              });
        StreamingUtils.printWithName(result, "result");

        List<Integer> collectedResult = ImmutableList.copyOf(DataStreamUtils.collect(result));
        // env.execute(); // DataStreamUtils.collect already call env.execute(), calling it
        // again here leads to exceptions

        // Two outputs are possible depending on whether the first element gets it's own
        // window or not, which is non deterministic
        List<Integer> expectedResult1 = ImmutableList.of(3, 7, 11); // windows (1,2) (3,4), (5,6)
        List<Integer> expectedResult2 = ImmutableList.of(1, 5, 9, 6); // windows (1) (2,3) (4, 5) (6)
        assertThat(collectedResult, anyOf(is(expectedResult1), is(expectedResult2)));
    }
}
