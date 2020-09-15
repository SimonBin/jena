package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.Collection;
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

}
