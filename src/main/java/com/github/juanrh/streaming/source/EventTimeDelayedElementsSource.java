package com.github.juanrh.streaming.source;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.NonNull;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * This class provides basically the same functionality as StreamExecutionEnvironment.fromCollection
 * but allows to specify a collection of elements together with a delay time when the element should be
 * generated, that is used to assign that as the event time of each element. Hence the DataStream
 * returned by get() should be used with env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
 *
 * That time is interpreted as a relative time, i.e. a time delta from an initial time
 * that is the the current time at the call of get() as returned by System.currentTimeMillis(). The
 * absolute time build as initial time + delta is assigned as the event time of the element
 *
 * NOTE this source doesn't delay the emission of elements, and instead elements are emitted instantaneously.
 * As a result the event time for an element might be later than the event ingestion time: we get early
 * events, instead of late events!
 * */
public class EventTimeDelayedElementsSource<T> implements Supplier<DataStream<T>> {
    private static final transient Logger LOG = LoggerFactory.getLogger(EventTimeDelayedElementsSource.class);

    @Data(staticConstructor="of")
    public static final class Elem<T> implements Serializable, Comparable<Elem<T>> {
        private static final long serialVersionUID = 1L;
        @NonNull private final T value;
        // Times are relative to an arbitrary start time
        @NonNull private final Time delay;

        @Override
        public int compareTo(Elem other) {
            long thisMillis = this.delay.toMilliseconds();
            long otherMillis = other.delay.toMilliseconds();
            return thisMillis < otherMillis ? -1 : (thisMillis == otherMillis ? 0 : 1);
        }
    }

    private final List<Elem<T>> elements;
    private final TypeInformation<T> elementsTypeInfo;
    private final StreamExecutionEnvironment env;

    /**
     * Time specified in elements are relative to a start time, that will be
     * the current time at the call of get() as returned by System.currentTimeMillis()
     * */
    public EventTimeDelayedElementsSource(StreamExecutionEnvironment env,
                                          Optional<TypeInformation<T>> elementsTypeInfo,
                                          final Iterable<Elem<T>> elements) {
        Preconditions.checkArgument(elements.iterator().hasNext(),
                "EventTimeDelayedElementsSource needs at least one element");
        this.elementsTypeInfo = SourceUtils.getTypeInformation(elementsTypeInfo,
                new Supplier<T>() {
                    @Override
                    public T get() {
                        return elements.iterator().next().getValue();
                    }
                });
        this.env = env;
        this.elements = Lists.newArrayList(elements);
    }

    public static <T> EventTimeDelayedElementsSource<T> withEqualGaps(StreamExecutionEnvironment env,
                                                                      final Time gap,
                                                                      T ... elements) {
        return withEqualGaps(env, gap, Lists.newArrayList(elements));
    }

    /**
     * @param gap relative gap between each element and the following
     * */
    public static <T> EventTimeDelayedElementsSource<T> withEqualGaps(StreamExecutionEnvironment env,
                                                                      final Time gap,
                                                                      Iterable<T> elements) {
        Preconditions.checkArgument(elements.iterator().hasNext(),
                "EventTimeDelayedElementsSource needs at least one element");
        List<Elem<T>> delayedElements = new LinkedList<>();
        long totalDelayMillis = 0L;
        for (T elem : elements) {
            delayedElements.add(Elem.of(elem, Time.milliseconds(totalDelayMillis)));
            totalDelayMillis += gap.toMilliseconds();
        }

        return new EventTimeDelayedElementsSource<>(
                env,
                Optional.<TypeInformation<T>>absent(), // FIXME
                delayedElements);
    }

    /**
     * The interval (every n milliseconds) in which the watermark will be generated
     * is defined via ExecutionConfig.setAutoWatermarkInterval
     * */
    @Override
    public DataStream<T> get() {
        return getEarly();
    }

    private DataStream<T> getLate() {
        long totalDelayMillis = 0L;
        for (Elem<T> elem : elements) {
            totalDelayMillis += elem.getDelay().toMilliseconds();
        }
        LOG.warn("totalDelayMillis = {}", totalDelayMillis);

        final long startTimestamp = System.currentTimeMillis() - totalDelayMillis;
        return env.fromCollection(elements)
           .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Elem<T>>(Time.milliseconds(totalDelayMillis*2)) {
               @Override
               public long extractTimestamp(Elem<T> elem) {
                   long timestamp = startTimestamp + elem.getDelay().toMilliseconds();
                   LOG.warn("Assigning timestamp of {}  to element {}",
                             timestamp,  elem.getValue());
                   return timestamp;
               }
           })
           .map(new MapFunction<Elem<T>, T>() {
               @Override
               public T map(Elem<T> elem) throws Exception {
                   return elem.getValue();
               }
           })
           .returns(elementsTypeInfo);
    }

    public DataStream<T> getEarly() {
        // sort so we can use AscendingTimestampExtractor
        Collections.sort(elements);

        final long startTimestamp = System.currentTimeMillis();
        return env.fromCollection(elements)
           .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Elem<T>>() {
               @Override
               public long extractAscendingTimestamp(Elem<T> elem) {
                        long timestamp = startTimestamp + elem.getDelay().toMilliseconds();
                        LOG.warn("Assigning timestamp of {}  to element {}",
                                timestamp,  elem.getValue());
                        return timestamp;
                    }
                })
                .map(new MapFunction<Elem<T>, T>() {
                    @Override
                    public T map(Elem<T> elem) throws Exception {
                        return elem.getValue();
                    }
                })
                .returns(elementsTypeInfo);
    }
}
