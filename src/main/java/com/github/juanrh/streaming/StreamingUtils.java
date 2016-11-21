package com.github.juanrh.streaming;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class StreamingUtils {
    public static <T> void printWithName(final DataStream<T> stream, final String name) {
        stream.map(new MapFunction<T, String>() {
            @Override
            public String map(T value) throws Exception {
                return name + ": " + value ;
            }
        }).print();
    }
}
