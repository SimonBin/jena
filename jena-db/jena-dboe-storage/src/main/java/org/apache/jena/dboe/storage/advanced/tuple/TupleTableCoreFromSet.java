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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <TupleType>
 * @param <ComponentType>
 */
public abstract class TupleTableCoreFromSet<TupleType, ComponentType>
    implements TupleTableCore<TupleType, ComponentType>
{
    protected Set<TupleType> set;

    public TupleTableCoreFromSet() {
        this (new LinkedHashSet<>());
    }

    public TupleTableCoreFromSet(Set<TupleType> set) {
        super();
        this.set = set;
    }


    @Override
    public void clear() {
        set.clear();
    }

    @Override
    public long size() {
        return set.size();
    }

    @Override
    public void add(TupleType tuple) {
        set.add(tuple);
    }

    @Override
    public void delete(TupleType tuple) {
        set.remove(tuple);
    }

    @Override
    public boolean contains(TupleType tuple) {
        return set.contains(tuple);
    }

    @Override
    public Stream<TupleType> findTuples() {
        return set.stream();
    }
}
