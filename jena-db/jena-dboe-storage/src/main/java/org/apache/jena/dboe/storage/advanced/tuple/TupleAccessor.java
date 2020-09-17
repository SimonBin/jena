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

/**
 * Access components of a domain object such as a Triple or Quad as if it was a Tuple
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <DomainType>
 */
public interface TupleAccessor<DomainType, ComponentType>
    extends TupleAccessorCore<DomainType, ComponentType>
{
    int getDimension();

    /**
     * Restore a domain object from some other object with a corresponding accessor
     * The length of the component array must equal the rank of the accessor
     *
     * @param <T> The type of tuple-like domain object from which to copy its components
     * @param obj The domain object
     * @param accessor The accessor of components from the domain object
     * @return
     */
    <T> DomainType restore(T obj, TupleAccessorCore<? super T, ? extends ComponentType> accessor);

    default void validateRestoreArg(TupleAccessor<?, ?> accessor) {
        int cl = accessor.getDimension();
        int r = getDimension();

        if (cl != r) {
            throw new IllegalArgumentException("components.length must equal rank but " + cl + " != " + r);
        }

    }

    default ComponentType[] toComponentArray(DomainType domainObject) {
        int rank = getDimension();
        @SuppressWarnings("unchecked")
        ComponentType[] result = (ComponentType[])new Object[rank];

        for (int i = 0; i < rank; ++i) {
            result[i] = get(domainObject, i);
        }

        return result;
    }

}
