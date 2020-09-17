/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 */
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
