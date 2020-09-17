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

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.jena.ext.com.google.common.collect.Streams;
import org.apache.jena.ext.com.google.common.graph.Traverser;

import com.github.andrewoma.dexx.collection.List;


/**
 * Breadth first search utilities for enumerating paths and indexed paths through a tree.
 * A path is a sequence of nodes represented using a persistent List
 * In an indexed path each node labeled with its index among the parents children.
 *
 * Useful for trees whose nodes do not link to their parents.
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class BreadthFirstSearchLib {

    /**
    *
    * @param <N>
    */
   public static <N> Stream<List<N>>
   breadthFirstPaths(List<N> basePath, Function<? super N, Stream<? extends N>> successorFn) {

       Supplier<Stream<List<N>>> breadth = () -> Stream.of(basePath).flatMap(parentPath ->
               successorFn.apply(parentPath.last()).map(childItem -> parentPath.append(childItem)));

       return Stream.concat(
               breadth.get(),
               breadth.get().flatMap(path -> breadthFirstPaths(path, successorFn)));
   }


   /**
    * Modified breadth first search that includes the indexes of the children
    *
    * @param <N>
    * @param basePath
    * @param successorFn
    * @return
    */
   public static <N> Stream<List<Entry<N, Integer>>>
   breadthFirstIndexedPaths(List<Entry<N, Integer>> basePath, Function<? super N, Stream<? extends N>> successorFn) {

       Supplier<Stream<List<Entry<N, Integer>>>> breath =
               () -> Stream.of(basePath).flatMap(parentPath ->
                         Streams.zip(
                                 successorFn.apply(parentPath.last().getKey()).map(x -> (N)x),
                                 IntStream.iterate(0, i -> i + 1).boxed(),
                                 (a, b) -> new SimpleEntry<>(a, b)
                        )
                        .map(childItem -> parentPath.append(childItem)));

       return Stream.concat(
               breath.get(),
               breath.get().flatMap(entry -> breadthFirstIndexedPaths(entry, successorFn)));
   }


   public static <N> N breadthFirstFindFirst(
           N start,
           Function<? super N, ? extends Iterable<? extends N>> successorFunction,
           Predicate<? super N> predicate
           ) {

       N result = null;
       Iterable<N> it = Traverser.<N>forTree(node -> successorFunction.apply(node)).breadthFirst(start);
       for (N node : it) {
           if (predicate.test(node)) {
               result = node;
               break;
           }
       }

       return result;
   }



   /**
    * Item must not be null!
    *
    * @param <N>
    * @param <C>
    * @param start
    * @param successorsFunction
    * @param wrapChild
    * @param predicate
    * @return
    */
   public static <N, C> N breadthFirstFindFirstIndirect(
           N start,
           Function<? super N, ? extends Iterable<? extends C>> successorsFunction,
           BiFunction<? super C, ? super N, ? extends N> wrapChild,
           BiPredicate<? super N, ? super N> predicate
           ) {

       N result = null;

       java.util.List<N> children = new ArrayList<>();
       for(C rawChild : successorsFunction.apply(start)) {
           N child = wrapChild.apply(rawChild, start);
           boolean matches = predicate.test(child, start);
           if (matches) {
               result = child;
               break;
           }

           children.add(child);
       }

       if (result == null) {
           for (N child : children) {
               result = breadthFirstFindFirstIndirect(child, successorsFunction, wrapChild, predicate);
               if (result != null) {
                   break;
               }
           }

       }

       return result;
   }

}
