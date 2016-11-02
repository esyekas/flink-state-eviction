package com.github.juanrh.streaming.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Either;
import org.apache.flink.api.common.time.Time;

import lombok.NonNull;

import com.twitter.chill.MeatLocker;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Optional;

/**
 * Source function similar to StreamExecutionEnvironment.fromElements but
 * that alternatively allows to add time based gaps between the generated
 * items
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
            this.typeInfo = Optional.empty();
        }
        public Builder(@NonNull TypeInformation<T> typeInfo) {
            this.typeInfo = Optional.of(typeInfo);
        }

        public Builder<T> addElem(@NonNull T elem) {
            elements.add(new MeatLocker<>(Either.Left(elem)));
            return this;
        }

        public Builder<T> addGap(@NonNull Time gap) {
            elements.add(new MeatLocker<>(Either.Right(gap)));
            return this;
        }

        private Optional<T> findAnyElem() {
            return elements.stream()
                            .map(MeatLocker::get)
                            .filter(Either::isLeft)
                            .map(Either::left)
                            .findAny();
        }

        /**
         * Build a ElementsWithGapsSource for the elements and gaps provided so far.
         * This checks the state of the builder and the ElementsWithGapsSource to be generated
         * are correct, and that we are able to generate the required TypeInformation for
         * the elements.
         * */
        public ElementsWithGapsSource<T> build() {
            Optional<T> someElem = findAnyElem();
            Preconditions.checkState(someElem.isPresent(),
                                     "ElementsWithGapsSource needs at least one elements that it's not a gap");
            TypeInformation<T> elementsTypeInfo = typeInfo.orElseGet(() -> {
                try {
                    return TypeExtractor.getForObject(someElem.get());
                } catch (Exception e) {
                     throw new RuntimeException("Could not create TypeInformation for type "
                                + someElem.get().getClass().getName()
                                + "; please specify the TypeInformation manually via "
                                + "ElementsWithGapsSource#addElem(TypeInformation, T) or "
                                + "ElementsWithGapsSource#addGap(TypeInformation, Time)");
                        }
                    }
            );
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
