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

package org.apache.jena.dboe.storage.quad;

import java.util.AbstractMap.SimpleEntry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.triple.TripleTableCore;
import org.apache.jena.dboe.storage.tuple.IndexNode;
import org.apache.jena.dboe.storage.tuple.IndexNodeFork;
import org.apache.jena.dboe.storage.tuple.IndexNodeForkFromMap;
import org.apache.jena.dboe.storage.tuple.IndexNodeNestedMap;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.NodeUtils;

/**
 * QuadTableCore implementation the maps graph names to {@link TripleTableCore}
 * The map and the supplier for TripleTableCore instances are configurable.
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class QuadTableCoreFromMapOfTripleTableCore
    implements QuadTableCore
{
    protected Map<Node, TripleTableCore> store;
    protected Supplier<? extends TripleTableCore> tripleTableSupplier;

    public QuadTableCoreFromMapOfTripleTableCore(
            Supplier<? extends TripleTableCore> tripleTableSupplier) {
        this(new LinkedHashMap<>(), tripleTableSupplier);
    }

    public QuadTableCoreFromMapOfTripleTableCore(
            Map<Node, TripleTableCore> store,
            Supplier<? extends TripleTableCore> tripleTableSupplier) {
        super();
        this.store = store;
        this.tripleTableSupplier = tripleTableSupplier;
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public void add(Quad quad) {
        TripleTableCore tripleTable = store.computeIfAbsent(quad.getGraph(), g -> tripleTableSupplier.get());
        tripleTable.add(quad.asTriple());
    }

    @Override
    public void delete(Quad quad) {
        TripleTableCore tripleTable = store.get(quad.getGraph());
        if (tripleTable != null) {
            tripleTable.delete(quad.asTriple());
        }
    }

    @Override
    public Stream<Quad> find(Node g, Node s, Node p, Node o) {
        return find(store, g, s, p, o);
    }

    @Override
    public IndexNodeFork<Node> getRootIndexNode(IndexNode<Node> parent) {
        return null;
//        return IndexNodeForkFromMap.singleton(
//            parent, 3, forkG -> IndexNodeNestedMap.create(
//                forkG, store, (nodeG, sMap) -> store.get(nodeG).getRootIndexNode(nodeG)));
    }


    @Override
    public Stream<Node> listGraphNodes() {
        return store.keySet().stream();
    }

    public static Stream<Quad> find(
            Map<Node, TripleTableCore> store,
            Node g, Node s, Node p, Node o)
    {
        Stream<Quad> result = matchEntries(store, NodeUtils::isNullOrAny, g)
                            .flatMap(e -> e.getValue().find(s, p, o).map(triple -> new Quad(e.getKey(), triple)));

        return result;
    }

    /**
     * Create a stream of matching entries in a map for a given key where the key
     * may be a wildcard
     *
     * @param <K>   The map's key type
     * @param <V>   The map's value type
     * @param in    A stream of input maps
     * @param isWildcard Predicate whether a key is concrete
     * @param k     A key
     * @return
     */
    public static <K, V> Stream<Entry<K, V>> matchEntries(Map<K, V> in, Predicate<? super K> isWildcard, K k) {
        boolean allKeys = isWildcard.test(k);
        Stream<Entry<K, V>> result = allKeys
                ? in.entrySet().stream()
                : in.containsKey(k)
                    ? Stream.of(new SimpleEntry<>(k, in.get(k)))
                    : Stream.empty();

        return result;
    }
}
