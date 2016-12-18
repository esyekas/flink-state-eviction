package com.github.juanrh.streaming;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.sun.xml.internal.bind.v2.model.core.TypeInfo;
import com.twitter.chill.MeatLocker;
import lombok.*;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by juanrh on 11/25/2016.
 *
 * TODO:
 *  - how to encapsulate: there is any equivalent to DataSet.runOperation for DataStream?
 *  - how to get the KeySelector from a KeyedStream? ==> see how they do it in mapWithState
 */
/*
* TODO extend like this was a method of KeyedDatastream
*  - take DataStream<T> as self in constructor
*  - have chained setter for other arguments: if default make sense; should feel like a map
* in the interface in the degenerate case
*  - ask for KeySelector for now
* */
//@RequiredArgsConstructor
@Accessors(fluent = true)
public class MapWithState<In, Key, State, Out> implements Supplier<SingleOutputStreamOperator<Out>>, Serializable  {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MapWithState.class);

    private final transient DataStream<In> input;
    private final MapWithState.Function<In, State, Out> mapFunction;
    private final State defaultState;

    @Setter @Getter @NonNull
    private transient Time ttl = null;
    @Setter @Getter @NonNull
    private transient Time ttlRefreshInterval = null;
    @Setter @Getter @NonNull
    private KeySelector<In, Key> keySelector = null;

//    @AllArgsConstructor
    private static class EitherKeySelector<In, Key> implements KeySelector<Either<In,Key>, Key>, ResultTypeQueryable {
//        @Getter
        private final MeatLocker<KeySelector<In, Key>> keySelector; // FIXME why MeatLocker required if KeySelector extends Serializable
//        @Getter
        private transient final TypeInformation typeInfo;
        public EitherKeySelector(KeySelector<In, Key> keySelector, TypeInformation typeInfo) {
            this.keySelector = new MeatLocker<>(keySelector);
            this.typeInfo = typeInfo;
        }

        @Override
        public Key getKey(Either<In, Key> either) throws Exception {
            if (either.isLeft()) {
                return keySelector.get().getKey(either.left());
            }
            return either.right();
        }

        @Override
        public TypeInformation getProducedType() {
            return typeInfo;
        }
    }

    // TODO follow https://github.com/google/guava/wiki/ReflectionExplained
    // and if necessary get the type token for the whole class and project the type variables
    private transient final TypeToken<Key> keyType = new TypeToken<Key>(getClass()) {};

    // TODO memoize in lazy memoized supplier?
    @Override
    public SingleOutputStreamOperator<Out> get() {
        Preconditions.checkState(ttl != null && ttl.toMilliseconds()> 0,
                                 "ttlMillis should be non null and greater than 0, %s found", ttl.toMilliseconds());
        // TODO other preconditions
        DataStream<Either<In, Key>> eitherInputOrTombstone =
                input.map(new MapFunction<In, Either<In, Key>>() {
                    @Override
                    public Either<In, Key> map(In in) throws Exception {
                        return Either.Left(in);
                    }
                })//;
        .returns(new EitherTypeInfo<>(types.typeInfoIn, types.typeInfoKey));
//        new EitherTypeInfo(Class.class.<In>cast(types.typeIn.getRawType()),
//                null);
                           //Class.types.typeKey.getRawType());
        // TODO: consider conversion utility from TypeToken to TypeInfo


            // InvalidTypesException: Type of TypeVariable 'In' in 'class com.github.juanrh.streaming.MapWithState'
            // could not be determined. This is most likely a type erasure problem. The type extraction currently
            // supports types with generic variables only in cases where all variables in the return type can be deduced from the input type(s).
           //.returns(new TypeHint<Either<In, Key>>() {});
            // InvalidTypesException: Cannot infer the type information from the class alone.
            // This is most likely because the class represents a generic type. In that case,please use the 'returns(TypeHint)' method instead.
        //.returns(Class.class.<Either<In, Key>>cast(types.typeEitherInKey.getRawType()));

        // FIXME make timeout configurable
        IterativeStream<Either<In, Key>> eitherInputOrTombstoneIter =
                eitherInputOrTombstone.iterate(Time.seconds(5).toMilliseconds());

        EitherKeySelector<In, Key> eitherKeySelector = new EitherKeySelector<>(keySelector, types.typeInfoKey);
        DataStream<Either<Out, Key>> trans =
                eitherInputOrTombstoneIter
                        // required to avoid java.lang.RuntimeException: State key serializer has not been configured
                        // in the config. This operation cannot use partitioned state.
                        .keyBy(eitherKeySelector
//                                new KeySelector<Either<In,Key>, Key>() {
//                            @Override
//                            public Key getKey(Either<In, Key> either) throws Exception {
//                                if (either.isLeft()) {
//                                    return keySelector.getKey(either.left());
//                                }
//                                return either.right();
//                            }
//                        }
                        )
                        //.flatMap(new CoreTransformationFunction())//;
                        .flatMap(new CoreTransformationFunction<>(
                                    ttl.toMilliseconds(),
                                    ttlRefreshInterval.toMilliseconds(),
                                    defaultState,
                                    mapFunction,
                                    keySelector
                                ))
                        .returns(new EitherTypeInfo<>(types.typeInfoOut, types.typeInfoKey));

        DataStream<Either<In, Key>> closing = trans.flatMap(new FlatMapFunction<Either<Out, Key>, Either<In, Key>>() {
            @Override
            public void flatMap(Either<Out, Key> either, Collector<Either<In, Key>> collector) throws Exception {
                if (either.isRight()) {
                    // collector.collect(either); // this fails
                    // this makes the type transformation, and that's why we cannot use a filter
                    collector.collect(Either.<In, Key>Right(either.right()));
                }
            }
        }).returns(new EitherTypeInfo<>(types.typeInfoIn, types.typeInfoKey));
        eitherInputOrTombstoneIter.closeWith(closing);
        SingleOutputStreamOperator<Out> output = trans.flatMap(new FlatMapFunction<Either<Out, Key>, Out>() {
            @Override
            public void flatMap(Either<Out, Key> either, Collector<Out> collector) throws Exception {
                if (either.isLeft()) {
                    collector.collect(either.left());
                }
            }
        }).returns(types.typeInfoOut);
        // this does nothing due to type erasure
        //.returns(TypeInformation.of(new TypeHint<Out>() {}));
        //.returns(Class.class.<Out>cast(types.typeOut.getRawType())); // ok

        return output;
    }

    private static class Types<In, Key, State, Out> {
        TypeToken<In> typeIn = new TypeToken<In>(getClass()) {};
        TypeToken<Key> typeKey = new TypeToken<Key>(getClass()) {};
        TypeToken<State> typeState= new TypeToken<State>(getClass()) {};
        TypeToken<Out> typeOut = new TypeToken<Out>(getClass()) {};
        TypeToken<Either<In, Key>> typeEitherInKey = new TypeToken<Either<In, Key>>(getClass()) {};

        TypeInformation<In> typeInfoIn = TypeInformation.of(Class.class.<In>cast(typeIn.getRawType()));
        TypeInformation<Key> typeInfoKey = TypeInformation.of(Class.class.<Key>cast(typeKey.getRawType()));
        TypeInformation<Out> typeInfoOut = TypeInformation.of(Class.class.<Out>cast(typeOut.getRawType()));
    }
    private transient final Types<In, Key, State, Out> types = new Types<In, Key, State, Out>(){} ;

    public MapWithState(DataStream<In> input,
                        MapWithState.Function<In, State, Out> mapFunction,
                        State defaultState){
        this.input = input;
        this.mapFunction = mapFunction;
        this.defaultState = defaultState;
    }

    public interface Function<In, State, Out> extends org.apache.flink.api.common.functions.Function {
        // FIXME add note about exception thrown just like https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/api/common/functions/MapFunction.html
        // and above all double check this behaves the same as that, i.e. "Throwing an exception will cause the operation to fail and may trigger recovery"
        // when used below
        Out map(In value, ValueState<State> state) throws Exception ; // HERE start writing a test to see if this interface works
    }


    // FIXME: make this truly non null by adding a Optional<T>, and remove the
    // default value and use Optional.absent instead
    /**
     * This class is only a mutable container for a value of type T, with
     * additional access info to implementing the time to live, and that
     * implements ValueState to have an interface familiar to users of
     * ValueState when implementing a MapWithStateFunction
     *
     * Note this object is in fact persisted in a ValueState<TimeStampedValue<T>> */
    public static class TimeStampedValue<T> implements Serializable, ValueState<T> {
        private static final long serialVersionUID = 1L;

        @NonNull
        private T value;
        /** millis since UNIX epoch like in System.currentTimeMillis */
        private long lastAccessTimestamp;
        private boolean firstTombtoneSent;

        public TimeStampedValue(@NonNull T value) {
            this.value = value;
            this.lastAccessTimestamp = 0;
            this.firstTombtoneSent = false;
        }

        @Override
        public T value() throws IOException {
            // update state for the current key was last touched now
            lastAccessTimestamp = System.currentTimeMillis();
            return value;
        }

        @Override
        public void update(T value) throws IOException {
            this.value = value;
        }

        @Override
        public void clear() {
            this.value = null;
        }
    }

    //@RequiredArgsConstructor
    public static class CoreTransformationFunction<In, Key, State, Out>  extends RichFlatMapFunction<Either<In, Key>, Either<Out, Key>>
                                            implements Serializable, Checkpointed<LinkedList<Key>> {
        private static final long serialVersionUID = 1L;

        private final long ttlMillis; // = MapWithState.this.ttl.toMilliseconds();
        private final long ttlRefreshIntervalMillis; // = MapWithState.this.ttlRefreshInterval.toMilliseconds();
        private final State defaultState;
            // FIMXE why MeatLocker when Function extends Serializable?
            // FIXME consider alternative solution based on RichFlatMapFunction init for this
        private final MeatLocker<MapWithState.Function<In, State, Out>> mapFunction;
        private final MeatLocker<KeySelector<In, Key>> keySelector;

        public CoreTransformationFunction(long ttlMillis, long ttlRefreshIntervalMillis,
                                          State defaultState,
                                          MapWithState.Function<In, State, Out> mapFunction,
                                          KeySelector<In, Key> keySelector) {
            this.ttlMillis = ttlMillis;
            this.ttlRefreshIntervalMillis = ttlRefreshIntervalMillis;
            this.defaultState = defaultState;
            this.mapFunction = new MeatLocker<>(mapFunction);
            this.keySelector = new MeatLocker<>(keySelector);

        }

        // FIXME consider replacing by Akka scheduler http://doc.akka.io/docs/akka/2.4.4/java/scheduler.html
        // if the ActorContext is available somehow
        private transient ValueState<TimeStampedValue<State>> valueState;
        private transient ScheduledExecutorService executor;
        private transient Set<Key> pendingTombstones;
        private transient Optional<List<Key>> recoveringTombstones;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TimeStampedValue<State>> descriptor =
                new ValueStateDescriptor<>("sumWithTimestamp", // the state name
                                           TypeInformation.of(new TypeHint<TimeStampedValue<State>>() {}),
                                           new TimeStampedValue<>(defaultState));
            valueState = getRuntimeContext().getState(descriptor);
            executor = Executors.newSingleThreadScheduledExecutor();
            pendingTombstones = Collections.newSetFromMap(new ConcurrentHashMap<Key, Boolean>());
            recoveringTombstones = Optional.absent();
        }

        @Override
        public void close() {
            // just shutdown ASAP, it's ok to lose the pending
            // tombstones as the state will be deleted anyway
            executor.shutdownNow();
            LOG.warn("Closed function");
        }

        private void sendTombstone(final Collector<Either<Out, Key>> collector, final Key key) {
            LOG.warn("Scheduling the shipment of a tombstone for key {}", key);
            pendingTombstones.add(key);
            executor.schedule(new Runnable() {
                @Override
                public void run() {
                    LOG.warn("Sending a tombstone for key {}", key);
                    Either<Out, Key> tombstone = Either.Right(key);
                    collector.collect(tombstone);
                    pendingTombstones.remove(key);
                }
            }, ttlRefreshIntervalMillis, TimeUnit.MILLISECONDS);
        }

        // FIXME handle IOException accessing the state
        @Override
        public void flatMap(Either<In, Key> either, Collector<Either<Out, Key>> collector) throws Exception {
            if (recoveringTombstones.isPresent()) {
                for (Key tombstone : recoveringTombstones.get()) {
                    LOG.warn("Recovering tombstone {}", tombstone);
                    sendTombstone(collector, tombstone);
                }
                recoveringTombstones = Optional.absent();
            }
            final TimeStampedValue<State> state = valueState.value();
            if (either.isLeft()) {
                In inputValue = either.left();
                // state can be mutated here by mapFunction, and access to
                // the state with value() will update state.lastAccessTimestamp
                // call this here before updating the state
                Out outputValue = //MapWithState.this.
                        mapFunction.get().map(inputValue, state);
                if (! state.firstTombtoneSent) {
                    // sent tombstone to the same key: this is only required to send the first tombstone
                    // FIXME: this only works if we have access to the key!!!
                    state.firstTombtoneSent = true;
                    sendTombstone(collector, //MapWithState.this.
                             keySelector.get().getKey(inputValue));
                }
                // store wrapping state in actual value state
                valueState.update(state);
                collector.collect(Either.<Out, Key>Left(outputValue));
            } else {
                // we just received a tombstone
                LOG.warn("Received a tombstone for key {}", either.right());
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis - state.lastAccessTimestamp >= ttlMillis) {
                    // evict and stop sending tombstones for this key
                    // next time this key appears and it uses the state, the default state
                    // that makes isTombstoneSent = false will be used, and that will send
                    // the first tombstone of the new eviction cycle
                    valueState.clear();
                    LOG.warn("Evicted state for key " + either.right());
                } else {
                    // send another tombstone: the current tombstone prevented sending
                    // more tombstone for events after the one that sent the current tombstone
                    sendTombstone(collector, either.right());
                }
            }
        }

        // FIXME handle IOException accessing the state
        @Override
        public LinkedList<Key> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
            LinkedList<Key> state = Lists.newLinkedList(pendingTombstones);
            if (recoveringTombstones.isPresent()){
                state.addAll(recoveringTombstones.get());
            }
            return state;
        }

        @Override
        public void restoreState(LinkedList<Key> tombstones) throws Exception {
            /* Send all the pending tombstone during the recovery, which is a good enough
            approximation even though would lead to sending the tombstones a little early */
            // only have access to the collector in flatMap, keep in recoveringTombstones
            // until next call to flatMap
            recoveringTombstones = Optional.<List<Key>>of(Lists.newLinkedList(tombstones));
        }
    }
}
