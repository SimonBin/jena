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

package org.apache.jena.sparql.service.enhancer.slice.impl;

public interface Buffer<A>
    extends BufferLike<A>
{
    /** Create a sub-buffer view of this buffer */
//    default Buffer<A> slice(long offset, long length) {
//        if (LongMath.checkedAdd(offset, length) > getCapacity()) {
//            throw new RuntimeException("Sub-buffer extends over capacity of this buffer");
//        }
//
//        // TODO If this buffer is already a sub-buffer then prevent wrapping it again for performance
//
//        return new SubBuffer<A>(this, offset, length);
//    }

    /**
     * Create a list over this buffer. The size of the list will be Ints.saturatedCast() of the buffer's capacity.
     * For this reason, it's recommended to use appropriately sliced buffers
     */
//    default <T> List<T> asList() {
//        return new ListOverBuffer<T>(this);
//    }
}
