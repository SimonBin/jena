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

package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamer;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamerFromTuple;
import org.apache.jena.graph.Node;

/**
 * Base interface for tuples i.e. triples and quads.
 * Evolving.
 * Deliberately analogous to {@link org.apache.jena.sparql.core.mem.TupleTable} to enable potential
 * future consolidation
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <TupleType> The type of the tuples to hold
 */
public interface TupleTableCore<TupleType, ComponentType>
    extends TupleQuerySupport<TupleType, ComponentType>
{
    /**
     * Clear all entries
     *
     * Optional operation. May fail with {@link UnsupportedOperationException} for e.g. views.
     */
    void clear();

    void add(TupleType tuple);
    void delete(TupleType tuple);
    boolean contains(TupleType tuple);

    // <T> Stream<TupleType> find(T lookup, TupleAccessor<? super T, ? extends ComponentType> accessor);

    /**
     * The basic find method that yields all tuples whose components equal
     * the corresponding non-null components in pattern.
     *
     * Be aware that the contract for this method is that only null values in the pattern
     * correspond to 'match any'
     *
     * Specifically, don't use Node.ANY here or it may result in an attempt to match tuples
     * with this value exactly. On the tuple-level we don't know or care about such domain conventions.
     *
     *
     * @param tupleTypes
     * @return
     */
    Stream<TupleType> findTuples(List<ComponentType> pattern);

    Stream<TupleType> findTuples();

    /**
     *
     *
     * @return
     */
    default ResultStreamer<TupleType, ComponentType, Tuple<ComponentType>> find(TupleQuery<ComponentType> tupleQuery) {
        List<ComponentType> pattern = tupleQuery.getPattern();

        // The projector is the function that projects a domain object into an appropriate tuple w.r.t.
        // the given projection indices
        int[] project = tupleQuery.getProject();
        Function<TupleType, Tuple<ComponentType>> projector = TupleOps.createProjector(project, getTupleAccessor());

        Supplier<Stream<Tuple<ComponentType>>> tupleStreamSupplier = () -> {
            Stream<TupleType> domainStream = findTuples(pattern);
            Stream<Tuple<ComponentType>> tupleStream = domainStream.map(projector::apply);

            if (tupleQuery.isDistinct()) {
                tupleStream = tupleStream.distinct();
            }
            return tupleStream;
        };

        return new ResultStreamerFromTuple<>(getDimension(), tupleStreamSupplier, getTupleAccessor());
    }

    /** Convenience fluent API */
    default TupleFinder<TupleType, TupleType, ComponentType> newFinder() {
        return TupleFinderImpl.create(this);
    }


    /**
     * The number of tuples in the table.
     * If not indexed then this value is used as the estimate of lookups
     * Therefore the size should be cached
     *
     * @return
     */
    default long size() {
        return find(new TupleQueryImpl<>(getDimension())).streamAsTuple().count();
    }

    /**
     * By default there is just the root index node which claims that
     * any lookup performs a full scan on the data
     *
     * @return
     */

    /**
     * The number of components / columns
     *
     * @return
     */
    default int getDimension() {
        return getTupleAccessor().getDimension();
    }


    TupleAccessor<TupleType, ComponentType> getTupleAccessor();



    // Does not belong here because tuples are generic and ANY is a domain value
    public static Node nullToAny(Node n) {
        return n == null ? Node.ANY : n;
    }

    public static Node anyToNull(Node n) {
        return Node.ANY.equals(n) ? null : n;
    }

}
