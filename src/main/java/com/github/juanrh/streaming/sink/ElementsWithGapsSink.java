package com.github.juanrh.streaming.sink;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Created by juanrh on 11/3/2016.
 *
 * Test sink that holds an expected list of elements with gaps
 *
 * Current state:
 *  - this doesn't work because Flink seems to create several instances of
 *  ElementsWithGapsSink and the state in expectedElements is replicated
 *  and not kept in sync accross instances. Some options:
 *     * change paralelism to ensure this has parallelism of 1
 *      ==> TODO understand Flink paralellism better and confirm this is why this behaves like this
 *     * use state to keep the list as a map from index to elem
 *
 *
 * TODO
 *  - first just check a sequence of expected values against the actually
 *  generated values, no time into account
 *  - make list list of expected elements paired with relative time as delay after
 *  stream start. Probably we need some tolerance to make this usable, as there
 *  is no sync like with micro batching. See time extraction in  https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/event_timestamp_extractors.html
 *  - implement Checkpointed
 *  - add builder to fix expectation before run, like in ElementsWithGapsSource
 *  - add option to skip putting time: could be optional, so it's only checked
 *  in present, and otherwise you only assert the element sequence but not the time
 *  - add option to specify same gap for elemens: just map on the previous list
 *  of pairs to add 0*gap, 1*gap, 2*gap, etc to each elem
 *  - define a harmcrest matcher for this: should be executed after the stream
 *  execution, could be returned in a method of this Sink
 *  - refine the harmcrest matcher so it fails fast, this could be done in
 *  2 ways: 1) complete evaluation of the matcher before the whole stream
 *  computation is complete; 2) stop the stream computation when the
 *  matcher fails (could be just launching the match failure exception)
 */
public class ElementsWithGapsSink<T>
        implements SinkFunction<T>, Serializable {
    //        Checkpointed<LinkedList<MeatLocker<Either<T, Time>>>> {

    @Getter private boolean matchingFailure = false ;
    private final LinkedList<T> expectedElements;

    public ElementsWithGapsSink(@NonNull Iterable<? extends T> expectedElems) {
        this.expectedElements = Lists.newLinkedList(expectedElems);
    }

    public static final Serializable FIXME = new Serializable() { } ;


    /*
    * This doesn't work because Sinks seem to be parallel by
    * default, and so there is one sink object per JVM with its own state,
    * and I need shared state here
    *
    * */
    @Override
    // sync on this doesn't work because there are  several instances of this class
    // in several threadss
//    public synchronized void invoke(T actualElement) throws Exception {// FIXME synchronized
    public void invoke(T actualElement) throws Exception {
        // This might at best work for a single JVM, as there is one Class instance per JVM
//        synchronized (FIXME) {
            // TODO: check documentation to understand the threading model
            // TODO: when adding checkpointing, check is some sync is needed like in
            // SourceFunction that are Checkpointed
            System.out.println("Found " + actualElement); // FIXME
            System.out.println("matchingFailure before " + matchingFailure); // FIXME
            System.out.println("expectedElements before " + Joiner.on(',').join(expectedElements));
            // TODO: show task id, I think I need to implement richfunction for that
            System.out.flush();
            if (!matchingFailure && expectedElements.size() > 0) {
                T expectedElement = expectedElements.poll();
                // FIXME: this could be more sophisticated
                if (!actualElement.equals(expectedElement)) {
                    matchingFailure = true;
                }
                System.out.println("matchingFailure after " + matchingFailure); // FIXME
                System.out.println("expectedElements after " + Joiner.on(',').join(expectedElements));
                System.out.flush();
                return;
            }
            // more actual elements than expected
            matchingFailure = true;
            System.out.println("matchingFailure after " + matchingFailure); // FIXME
            System.out.println("expectedElements after " + Joiner.on(',').join(expectedElements));
            System.out.flush();
        }
//    }
}
