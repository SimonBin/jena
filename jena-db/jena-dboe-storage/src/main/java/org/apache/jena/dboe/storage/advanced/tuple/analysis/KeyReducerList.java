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

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;

import com.github.andrewoma.dexx.collection.List;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <C>
 */
public class KeyReducerList<C>
    implements KeyReducer<List<C>>
{
    protected StoreAccessor<?, C> accessor;
    protected StorageNode<?, C, ?> storage;
    protected int[] keyComponentsToAppend; // indexes pointing into storage.getKeyTupleIdxs()

    public KeyReducerList(
            StoreAccessor<?, C> accessor, int[] keyComponentsToAppend) {
        super();
        this.accessor = accessor;
        this.storage = accessor.getStorage();
        this.keyComponentsToAppend = keyComponentsToAppend;
    }

    @Override
    public List<C> reduce(List<C> accumulator, Object key) {
        for (int i = 0; i < keyComponentsToAppend.length; ++i) {
            int componentIdx = keyComponentsToAppend[i];

            C value = storage.getKeyComponentRaw(key, componentIdx);
            accumulator = accumulator.append(value);
        }

        return accumulator;
    }

}
