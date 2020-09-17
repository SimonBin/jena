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

/**
 * Accumulate a value from an object at a given index.
 * The object can be seen as the value of a component in a conceptual tuple.
 *
 * Note that the use of this interface is primarily for reducing keys in index nodes
 * The interpretation of keys - especially which tuple components can be extracted from it - depends on the index.
 *
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <K>
 */
public interface IndexedKeyReducer<K>
{
    K reduce(K accumulator, int indexNode, Object value);
}
