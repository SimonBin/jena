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
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.StoreAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.StoreAccessorImpl;
import org.apache.jena.dboe.storage.advanced.tuple.api.TupleFinder;
import org.apache.jena.dboe.storage.advanced.tuple.api.TupleQuery;
import org.apache.jena.dboe.storage.advanced.tuple.api.TupleTableCore;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TupleCodec;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNodeBased;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNodeMutable;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamer;


/**
*
*
* @author Claus Stadler 11/09/2020
*
* @param <D> Domain tuple type such as Triple, Quad, Tuple, List, etc
* @param <C> Component type such as Node
* @param <V> Store type such as a Map<Node, Set<Triple>>
*/
public abstract class TupleTableFromStorageNodeWithCodecBase<D1, C1, D2, C2, V>
   implements TupleTableCore<D1, C1>, StorageNodeBased<D2, C2, V>
{
    /** convert domain tuples to those supported by the storage */
    protected TupleCodec<D1, C1, D2, C2> tupleCodec;
    protected StorageNodeMutable<D2, C2, V> rootStorageNode;
    protected V store;

    protected StoreAccessor<D2, C2> storeAccessor;

    public TupleTableFromStorageNodeWithCodecBase(
            TupleCodec<D1, C1, D2, C2> tupleCodec,
            StorageNodeMutable<D2, C2, V> rootStorageNode,
            V store) {
        super();
        this.tupleCodec = tupleCodec;
        this.rootStorageNode = rootStorageNode;
        this.store = store;

        storeAccessor = StoreAccessorImpl.createForStorage(rootStorageNode);
   }

    public TupleCodec<D1, C1, D2, C2> getTupleCodec() {
        return tupleCodec;
    }

   @Override
    public StorageNodeMutable<D2, C2, V> getStorageNode() {
        return rootStorageNode;
    }

   @Override
    public V getStore() {
        return store;
    }

    @Override
    public void clear() {
        rootStorageNode.clear(store);
    }

    @Override
    public void add(D1 domainTuple) {
        D2 storageTuple = tupleCodec.encodeTuple(domainTuple);
        rootStorageNode.add(store, storageTuple);
    }

    @Override
    public void delete(D1 domainTuple) {
        D2 storageTuple = tupleCodec.encodeTuple(domainTuple);
        rootStorageNode.remove(store, storageTuple);
    }

    @Override
    public Stream<D1> findTuples(List<C1> pattern) {
        TupleFinder<D1, D1, C1> finder = newFinder();
        for (int i = 0; i < pattern.size(); ++i) {
            finder.eq(i, pattern.get(i));
        }

        return finder.stream();
    }

    @Override
    public ResultStreamer<D1, C1, Tuple<C1>> find(TupleQuery<C1> tupleQuery) {

        // TODO We need to put the codec in between
        throw new UnsupportedOperationException();

//       NodeStats<D, C> bestMatch = TupleQueryAnalyzer.analyze(tupleQuery, storeAccessor);
//       ResultStreamerBinder<D, C, Tuple<C>> binder = TupleQueryAnalyzer.createResultStreamer(
//               bestMatch,
//               tupleQuery,
//               getTupleAccessor());
//
//       return binder.bind(store);
    }
}
