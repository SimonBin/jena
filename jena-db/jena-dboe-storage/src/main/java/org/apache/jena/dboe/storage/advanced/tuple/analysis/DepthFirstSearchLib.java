package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class DepthFirstSearchLib {
    public static <N> Stream<N> conditionalDepthFirstInOrder(
            N start,
            Function<? super N, ? extends Collection<? extends N>> successorsFunction,
            Predicate<? super N> predicate) {

        return predicate.test(start)
            ? Stream.of(start)
            : successorsFunction.apply(start).stream()
                    .flatMap(child -> conditionalDepthFirstInOrder(child, successorsFunction, predicate));
    }

    public static <N> Stream<N> conditionalDepthFirstInOrderWithParent(
            N start,
            N parent,
            Function<? super N, ? extends Collection<? extends N>> successorsFunction,
            BiPredicate<? super N, ? super N> stopIfTrue) {

        return parent != null && stopIfTrue.test(start, parent)
            ? Stream.of(start)
            : successorsFunction.apply(start).stream()
                    .flatMap(child -> conditionalDepthFirstInOrderWithParent(child, start, successorsFunction, stopIfTrue));
    }


    /**
     * Given a node of type N obtain children of type C
     * then invoke a wrapping function that turns each C to an N again
     *
     *
     * @param <N>
     * @param start
     * @param parent
     * @param successorsFunction
     * @param stopIfNonNullResult
     * @return
     */
    public static <N, C> Stream<N> conditionalDepthFirstInOrderWithParentAndIndirectChildren(
            N start,
            N parent,
            Function<? super N, ? extends Collection<? extends C>> successorsFunction,
            BiFunction<? super C, ? super N, ? extends N> wrapperFn,
            BiPredicate<? super N, ? super N> stopIfTrue
            ) {

        return parent != null && stopIfTrue.test(start, parent)
            ? Stream.of(start)
            : successorsFunction.apply(start).stream()
                    .map(child -> wrapperFn.apply(child, start))
                    .flatMap(child -> conditionalDepthFirstInOrderWithParentAndIndirectChildren(child, start, successorsFunction, wrapperFn, stopIfTrue));
    }

}
