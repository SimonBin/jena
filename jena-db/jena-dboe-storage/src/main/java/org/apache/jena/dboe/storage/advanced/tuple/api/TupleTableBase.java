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

package org.apache.jena.dboe.storage.advanced.tuple.api;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <TupleType>
 * @param <ComponentType>
 */
public abstract class TupleTableBase<TupleType, ComponentType>
    implements TupleTableCore<TupleType, ComponentType>
{
    @Override
    public final Stream<TupleType> findTuples(List<ComponentType> pattern) {
//        assertEqual(getRank(), pattern.length, "Pattern length must be equal to columns in tuple table");
        return checkedFindTuples(pattern);
    }

    @SuppressWarnings("unchecked")
    protected abstract Stream<TupleType> checkedFindTuples(List<ComponentType> pattern);


    public static void assertEqual(Object a, Object b, String message) {
        if (!Objects.equals(a, b)) {
            throw new IllegalArgumentException(message + "; " + a + " != " + b);
        }
    }
}
