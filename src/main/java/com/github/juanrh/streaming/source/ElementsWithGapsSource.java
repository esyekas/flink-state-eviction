package com.github.juanrh.streaming.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.shaded.com.google.common.reflect.TypeToken;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Either;
import org.apache.flink.api.common.time.Time;

import lombok.NonNull;

import com.twitter.chill.MeatLocker;

import java.io.Serializable;
import java.util.LinkedList;

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

    // FIXME replace by typeInfo = TypeExtractor.getForObject(data[0]);
    // as in https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/environment/StreamExecutionEnvironment.java#L688
    // https://github.com/google/guava/wiki/ReflectionExplained
    private transient final TypeToken<T> elemsType = new TypeToken<T>(getClass()) {};

    private LinkedList<MeatLocker<Either<T, Time>>> elements = new LinkedList<>();
    private volatile boolean isRunning = true;
    private final Serializable waitForTime = new Serializable() { };

    public ElementsWithGapsSource<T> addElem(@NonNull T elem) {
        elements.add(new MeatLocker<>(Either.Left(elem)));
        return this;
    }

    public ElementsWithGapsSource<T> addGap(@NonNull Time gap) {
        elements.add(new MeatLocker<>(Either.Right(gap)));
        return this;
    }

    // -----------------------------
    // SourceFunction<T>
    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (isRunning && !elements.isEmpty()) {
            synchronized (ctx.getCheckpointLock()) {
                Either<T, Time> next = elements.poll().get(); //elements.poll(); // null pointer as transient
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
        return TypeInformation.of(Class.class.<T>cast(elemsType.getRawType()));
    }
}
