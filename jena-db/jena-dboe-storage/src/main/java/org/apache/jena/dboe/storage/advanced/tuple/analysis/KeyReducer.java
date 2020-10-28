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
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <K>
 */
public interface KeyReducer<K> {
    K reduce(K accumulator, Object contribution);


    /**
     * KeyReducer that returns the accumulator directly and thus
     * ignores the contribution.
     */
    static <K> K passThrough(K accumulator, Object contribution) {
        return accumulator;
    }
}
