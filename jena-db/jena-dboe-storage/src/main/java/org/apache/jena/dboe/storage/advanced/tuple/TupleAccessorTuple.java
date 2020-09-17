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

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <ComponentType>
 */
public class TupleAccessorTuple<ComponentType>
    implements TupleAccessor<Tuple<ComponentType>, ComponentType>
{
    protected int rank;

    public TupleAccessorTuple(int rank) {
        super();
        this.rank = rank;
    }

    @Override
    public int getDimension() {
        return rank;
    }

    @Override
    public ComponentType get(Tuple<ComponentType> domainObject, int idx) {
        return domainObject.get(idx);
    }

    @Override
    public <T> Tuple<ComponentType> restore(T obj, TupleAccessorCore<? super T, ? extends ComponentType> accessor) {
//        validateRestoreArg(accessor);

        List<ComponentType> xs = new ArrayList<>(rank);
        for (int i = 0; i < rank; ++i) {
            xs.set(i, accessor.get(obj, i));
        }
        return TupleFactory.create(xs);
//        ComponentType[] xs = accessor.toComponentArray(obj);
    }

}
