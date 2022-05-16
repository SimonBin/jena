/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.service.enhancer.impl;

import java.util.NavigableMap;

/** A set of items with an integer id. Any addition must be performed with an id that is strictly higher than
 * any id already in the batch. Inserted ids need not be consecutive. */
interface Batch<K extends Comparable<K>, T> {
    NavigableMap<K, T> getItems();
    void put(K index, T item);
    K getNextValidIndex();
    boolean isEmpty();
    int size();
}
