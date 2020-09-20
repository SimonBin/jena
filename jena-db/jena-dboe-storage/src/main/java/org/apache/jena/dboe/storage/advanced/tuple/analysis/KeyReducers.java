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

import java.util.Map.Entry;

import org.apache.jena.ext.com.google.common.collect.Maps;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class KeyReducers {
    public static final IndexedKeyReducer<Entry<?, ?>, Object> toPairs = (p, i, k) -> Maps.immutableEntry(p, k);

//    public static final KeyReducer<Entry<?, ?>> ignoreAll = (p, i, k) -> Maps.immutableEntry(p, k);


    /**
     * Simple reducer
     *
     *
     * @param <C>
     * @param idx
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <C> IndexedKeyReducer<C, Object> projectOnly(int idx) {
        return (acc, i, obj) -> {
            // If a non-null value has been accumulated then pass it on
            // Otherwise check if the obj at index i match idx - if so return it as the accumulated value
            C r = acc != null
                    ? acc
                    : i == idx
                        ? (C)obj
                        : null;
            return r;
        };
    }

    static <K> K passOn(K accumulator, int i, Object contribution) {
        return accumulator;
    }


}
