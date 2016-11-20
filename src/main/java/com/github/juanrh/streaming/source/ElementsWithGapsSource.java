package com.github.juanrh.streaming.source;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Either;
import org.apache.flink.api.common.time.Time;

import lombok.NonNull;

import com.twitter.chill.MeatLocker;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Source function similar to StreamExecutionEnvironment.fromElements but
 * that alternatively allows to add time based gaps between the generated
 * items
 *
 * I think this works because this source function it's not parallel, so there is
 * a single instance of ElementsWithGapsSource that holds the state
 *
 * Created by juanrh on 10/31/2016.
 */
public class ElementsWithGapsSource<T extends Serializable>
        implements SourceFunction<T>, Serializable,
                   Checkpointed<LinkedList<MeatLocker<Either<T, Time>>>>,
                   ResultTypeQueryable<T> {
    public static final long serialVersionUID = 1;

    public static class Builder<T extends Serializable> {
        private LinkedList<MeatLocker<Either<T, Time>>> elements = new LinkedList<>();
        private final Optional<TypeInformation<T>> typeInfo;

        public Builder() {
            this.typeInfo = Optional.absent();
        }
        public Builder(@NonNull TypeInformation<T> typeInfo) {
            this.typeInfo = Optional.of(typeInfo);
        }

        public Builder<T> addElem(@NonNull T elem) {
            elements.add(new MeatLocker<>(Either.<T, Time>Left(elem)));
            return this;
        }

        public Builder<T> addGap(@NonNull Time gap) {
            elements.add(new MeatLocker<>(Either.<T, Time>Right(gap)));
            return this;
        }

        private Optional<T> findAnyElem() {
            return FluentIterable.from(elements)
                    .transform(new Function<MeatLocker<Either<T,Time>>, Either<T, Time>>() {
                        @Override
                        public Either<T, Time> apply(MeatLocker<Either<T, Time>> eitherMeatLocker) {
                            return eitherMeatLocker.get();
                        }
                    })
                    .firstMatch(new Predicate<Either<T, Time>>() {
                        @Override
                        public boolean apply(Either<T, Time> tTimeEither) {
                            return tTimeEither.isLeft();
                        }
                    })
                    .transform(new Function<Either<T,Time>, T>() {
                        @Override
                        public T apply(Either<T, Time> tTimeEither) {
                            return tTimeEither.left();
                        }
                    });
        }

        /**
         * Build a ElementsWithGapsSource for the elements and gaps provided so far.
         * This checks the state of the builder and the ElementsWithGapsSource to be generated
         * are correct, and that we are able to generate the required TypeInformation for
         * the elements.
         * */
        public ElementsWithGapsSource<T> build() {
            final Optional<T> someElem = findAnyElem();
            Preconditions.checkState(someElem.isPresent(),
                                     "ElementsWithGapsSource needs at least one element that it's not a gap");
            final TypeInformation<T> elementsTypeInfo =
                SourceUtils.getTypeInformation(typeInfo, someElem.get());
            return new ElementsWithGapsSource<>(elementsTypeInfo, elements);
        }
    }

    // only modified by this source function during run(), or restoreState()
    private LinkedList<MeatLocker<Either<T, Time>>> elements;
    private final TypeInformation<T> elementsTypeInfo;

    private volatile boolean isRunning = true;
    private final Serializable waitForTime = new Serializable() {};

    ElementsWithGapsSource(TypeInformation<T> elementsTypeInfo, LinkedList<MeatLocker<Either<T, Time>>> elements) {
        this.elementsTypeInfo = elementsTypeInfo;
        this.elements = elements;
    }

    public static <T extends Serializable> Builder<T> addElem(@NonNull T elem) {
        return new Builder().addElem(elem);
    }

    public static <T extends Serializable> Builder<T> addElem(@NonNull TypeInformation<T> typeInfo, @NonNull T elem) {
        return new Builder(typeInfo).addElem(elem);
    }

    public static <T extends Serializable> Builder<T> addGap(@NonNull Time gap) {
        return new Builder().addGap(gap);
    }

    public static <T extends Serializable> Builder<T> addGap(@NonNull TypeInformation<T> typeInfo, @NonNull Time gap) {
        return new Builder(typeInfo).addGap(gap);
    }

    // -----------------------------
    // SourceFunction<T>
    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        // state was already checked in the builder before run
        while (isRunning && !elements.isEmpty()) {
            /* https://ci.apache.org/projects/flink/flink-docs-master/api/java/index.html?org/apache/flink/streaming/api/functions/sink/DiscardingSink.html
            * Sources that also implement the Checkpointed interface must ensure
            * that state checkpointing, updating of internal state and emission
            * of elements are not done concurrently.
            * */
            synchronized (ctx.getCheckpointLock()) {
                Either<T, Time> next = elements.poll().get();
                if (next.isLeft()) {
                    ctx.collect(next.left());
                } else {
                    synchronized (waitForTime) {
                        waitForTime.wait(next.right().toMilliseconds());
                    }
                }
            }
        }
    }

    @Override
    public void cancel() {
        synchronized (waitForTime) {
            waitForTime.notify();
        }
        isRunning = false;
    }

    // -----------------------------
    // Checkpointed<LinkedList<MeatLocker<Either<T, Time>>>>
    @Override
    public LinkedList<MeatLocker<Either<T, Time>>> snapshotState(long l, long l1) throws Exception {
       return elements;
    }

    @Override
    public void restoreState(LinkedList<MeatLocker<Either<T, Time>>> eithers) throws Exception {
        elements = new LinkedList<>(eithers);
    }

    // -----------------------------
    // ResultTypeQueryable<T>
    @Override
    public TypeInformation<T> getProducedType() {
        return elementsTypeInfo;
    }
}
