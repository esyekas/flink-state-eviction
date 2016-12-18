package com.github.juanrh.streaming;

import com.github.juanrh.streaming.source.ElementsWithGapsSource;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


/**
 * Created by juanrh on 11/28/2016.
 */
public class MapWithStateTest extends StreamingMultipleProgramsTestBase {
    public static StreamExecutionEnvironment getEnv() {
        // get shared execution context
        // https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/util/StreamingMultipleProgramsTestBase.html
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());
        env.getConfig().disableSysoutLogging();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime); // activate ingestion time
        return env;
    }

    @Test
    public void pairsValueState_MapWithState_StateIsEvictedCorrectly() throws Exception {
        final StreamExecutionEnvironment env = getEnv();

        SourceFunction<Tuple2<String, Integer>> source =
                ElementsWithGapsSource
                        .addElem(Tuple2.of("a", 2)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("a", 3)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("b", 1)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("c", 5)).addElem(Tuple2.of("d", 2)).addGap(Time.seconds(1))
                        .addElem(Tuple2.of("c", 2)).addGap(Time.seconds(1)).addElem(Tuple2.of("a", 3))
                        .addGap(Time.seconds(2)).addElem(Tuple2.of("c", 1)).build();

        DataStream<Tuple2<String, Integer>> input = env.addSource(source);

        final MapWithState<Tuple2<String, Integer>, String, Integer, Tuple2<String, Integer>> mapWithState =
            new MapWithState<Tuple2<String, Integer>, String, Integer, Tuple2<String, Integer>>(
                input,
                new MapWithState.Function<Tuple2<String, Integer>, Integer, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value, ValueState<Integer> state) throws Exception {
                        final int currentSum = value.f1 + state.value();
                        state.update(currentSum);
                        return Tuple2.of(value.f0, currentSum);
                    }
                },
                0) // default state
                .ttl(Time.milliseconds(1100)) // how long a state key will last in processing time (approx)
                .ttlRefreshInterval(Time.milliseconds(450)) // how often access to each state key will be checked (approx)
                .keySelector(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                });
        DataStream<Tuple2<String, Integer>> mapped = mapWithState.get();
        // TODO collect and assert
        // TODO ensure logging is customized to see all intermediate data as in the PoC
        StreamingUtils.printWithName(mapped, "mapped");

        List<Tuple2<String, Integer>> collectedMapped = ImmutableList.copyOf(DataStreamUtils.collect(mapped));
        //env.execute(); // DataStreamUtils.collect already call env.execute(), calling it
                         // again here leads to exceptions

        List<Tuple2<String, Integer>> expectedPrefix =
                ImmutableList.of(Tuple2.of("a", 2), Tuple2.of("a", 5), Tuple2.of("b", 1));
        List<Tuple2<String, Integer>> expectedSuffix =
                ImmutableList.of(Tuple2.of("c", 7), Tuple2.of("a", 3), Tuple2.of("c", 1));


        List<Tuple2<String, Integer>> expectedResult1 =
                ImmutableList.<Tuple2<String, Integer>>builder()
                        .addAll(expectedPrefix)
                        .add(Tuple2.of("c", 5)).add(Tuple2.of("d", 2))
                        .addAll(expectedSuffix)
                        .build();
        List<Tuple2<String, Integer>> expectedResult2 =
                ImmutableList.<Tuple2<String, Integer>>builder()
                        .addAll(expectedPrefix)
                        .add(Tuple2.of("d", 2)).add(Tuple2.of("c", 5))
                        .addAll(expectedSuffix)
                        .build();
        assertThat(collectedMapped, anyOf(is(expectedResult1), is(expectedResult2)));
    }
}
