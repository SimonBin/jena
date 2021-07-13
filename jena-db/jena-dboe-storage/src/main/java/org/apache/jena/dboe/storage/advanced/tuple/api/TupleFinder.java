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

import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <ExposedType>
 * @param <DomainType>
 * @param <ComponentType>
 */
public interface TupleFinder<ExposedType, DomainType, ComponentType> {

    /**
     * The number of columns of the underlying tuple source
     *
     * @return
     */
    int getDimension();

    /**
     * Set an equals constraint on a component
     *
     * @param componentIdx
     * @param value
     * @return
     */
//    TupleFinder<DomainType, ComponentType> eq(int componentIdx, ComponentType value);
//
//    TupleFinder<DomainType, ComponentType> distinct(boolean onOrOff);

    /**
     * If the tuple finder is already list-based to nothing
     * Otherwise
     *
     * @return
     */
    TupleFinder<Tuple<ComponentType>, DomainType, ComponentType> tuples();

    TupleFinder<ComponentType, DomainType, ComponentType> plain();

    TupleFinder<ExposedType, DomainType, ComponentType> distinct(boolean OnOrOff);

    default TupleFinder<ExposedType, DomainType, ComponentType> distinct() {
        return distinct(true);
    }

    /**
     * Add a specific component to the projection
     *
     * @param componentIdx
     * @return
     */
    TupleFinder<Tuple<ComponentType>, DomainType, ComponentType> project(int... componentIdx);

    /**
     * Remove a component from the projection
     *
     * @param componentIdx
     * @return
     */
//    TupleFinder<Tuple<ComponentType>, ComponentType> unproject(int componentIdx);

    /**
     * Whether the current TupleType can be viewed as instance of another class.
     * For example, any List<Node> with three components can be viewed as a Triple.
     *
     * @param viewClass
     * @return
     */
    boolean canAs(Class<?> viewClass);

    <T> T as(Class<T> viewClass);

    /**
     * Execute the request
     *
     * @return
     */
    // ExtendedIterator<DomainType> exec();

    Stream<ExposedType> stream();

    TupleFinder<ExposedType, DomainType, ComponentType> eq(int componentIdx, ComponentType value);

    TupleQuery<ComponentType> getTupleQuery();

    default TupleFinder<ComponentType, DomainType, ComponentType> projectOnly(int componentIdx) {
        return project(componentIdx).plain();
    }

}