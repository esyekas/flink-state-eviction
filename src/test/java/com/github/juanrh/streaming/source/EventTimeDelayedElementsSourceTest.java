package com.github.juanrh.streaming.source;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Test;

import java.util.List;

/*
Following
    https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/util/StreamingMultipleProgramsTestBase.html
*/
public class EventTimeDelayedElementsSourceTest  extends StreamingMultipleProgramsTestBase {
    final List<Integer> inputList = ContiguousSet.create(Range.closed(0, 10), DiscreteDomain.integers()).asList();

    public static StreamExecutionEnvironment getEnv() {
        // get shared execution context
        // https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/util/StreamingMultipleProgramsTestBase.html
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());
        env.getConfig().disableSysoutLogging();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // activate ingestion time
        return env;
    }

    /**
     * Early events seem to be interpreted as events generated at the
     * current processing time, so this just prints "55" which is the
     * result of adding all the events
     * */
    @Test
    public void fixme() throws Exception {
        final StreamExecutionEnvironment env = getEnv();

        DataStream<Integer> input =
                EventTimeDelayedElementsSource.withEqualGaps(env, Time.milliseconds(500), inputList).get();
        input.timeWindowAll(Time.seconds(1))
              .reduce(new ReduceFunction<Integer>() {
                  @Override
                  public Integer reduce(Integer i1, Integer i2) throws Exception {
                      return i1 + i2;
                  }
              })
            .print();

        env.execute();
    }
}
