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

package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.ext.com.google.common.collect.Maps;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <D>
 * @param <C>
 * @param <V1>
 * @param <V2>
 */
public class StorageNodeAlt2<D, C, V1, V2>
    extends StorageNodeNoKeyBase<D, C, Entry<V1, V2>>
    implements StorageNodeMutable<D, C, Entry<V1, V2>>
{
    // protected List<? extends Meta2NodeCompound<D, C, ?>> children;
    protected Entry<? extends StorageNodeMutable<D, C, V1>, ? extends StorageNodeMutable<D, C, V2>> children;

    public StorageNodeAlt2(
            TupleAccessor<D, C> tupleAccessor,
            StorageNodeMutable<D, C, V1> child1,
            StorageNodeMutable<D, C, V2> child2
        ) {
        super(tupleAccessor);
        this.children = Maps.immutableEntry(child1, child2);
    }

    @Override
    public List<? extends StorageNode<D, C, ?>> getChildren() {
        return Arrays.asList(children.getKey(), children.getValue());
    }

    @Override
    public <T> Stream<?> streamEntries(Entry<V1, V2> childStores, T tupleLike,
            TupleAccessorCore<? super T, ? extends C> tupleAccessor) {

        StorageNodeMutable<D, C, ?> pickedChild = children.getKey();
        Object pickedChildStore = childStores.getKey();

        // Delegate always to the first entry - we would need external information to do better
        return pickedChild.streamEntriesRaw(pickedChildStore, tupleLike, tupleAccessor);
    }

    /**
     * Return of a list with fresh stores of all children
     *
     */
    @Override
    public Entry<V1, V2> newStore() {
        return Maps.immutableEntry(children.getKey().newStore(), children.getValue().newStore());
    }

    /**
     * Checks whether all child store entries in the list of alternatives are empty
     *
     * (Not to be confused with checking the list of alternatives itself for emptiness)
     */
    @Override
    public boolean isEmpty(Entry<V1, V2> childStores) {
        StorageNodeMutable<D, C, ?> pickedChild = children.getKey();
        Object pickedChildStore = childStores.getKey();

        boolean result = pickedChild.isEmptyRaw(pickedChildStore);
        return result;
    }

    @Override
    public boolean add(Entry<V1, V2> childStores, D tupleLike) {
        boolean result = false;
        result = result || children.getKey().add(childStores.getKey(), tupleLike);
        children.getValue().add(childStores.getValue(), tupleLike);

        return result;
    }

    @Override
    public boolean remove(Entry<V1, V2> childStores, D tupleLike) {
        boolean result = children.getKey().remove(childStores.getKey(), tupleLike);
        children.getValue().remove(childStores.getValue(), tupleLike);

        return result;
    }

    @Override
    public Object chooseSubStore(Entry<V1, V2> store, int subStoreIdx) {
        Object result;
        switch(subStoreIdx) {
        case 0: result = store.getKey(); break;
        case 1: result = store.getValue(); break;
        default: throw new IndexOutOfBoundsException("Index must be 0 or 1; was " + subStoreIdx);
        }
        return result;
    }

    @Override
    public void clear(Entry<V1, V2> store) {
        children.getKey().clear(store.getKey());
        children.getValue().clear(store.getValue());
    }

//    @Override
//    public <T> Streamer<Entry<V1, V2>, ? extends Entry<?, ?>> streamerForKeyAndSubStores(
//            int altIdx,
//            T pattern,
//            TupleAccessorCore<? super T, ? extends C> accessor) {
//        return argStore -> Stream.of(Maps.immutableEntry(TupleFactory.create0(), chooseSubStore(argStore, altIdx)));
//    }


    @Override
    public String toString() {
        return "<" + getChildren().stream().map(Object::toString).collect(Collectors.joining(" | ")) + ">";
    }
}