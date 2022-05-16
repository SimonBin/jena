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

// Execution of the returned request is guaranteed to cover at least the next fetchAhead inputs
// Usually it will cover more than that; when getting close to the getFirstMissId()
// the driver may decide to send a further request
public class ServiceBatchRequestImpl<G, I>
    implements ServiceBatchRequest<G, I>
{
    protected G groupKey;
    protected Batch<Long, I> batch;
    // protected NavigableMap<Long, List<I>> schedule;

    public ServiceBatchRequestImpl(G groupKey, Batch<Long, I> batch) {
        super();
        this.groupKey = groupKey;
        this.batch = batch;
    }

    @Override
    public G getGroupKey() {
        return groupKey;
    }

    @Override
    public Batch<Long, I> getBatch() {
        return batch;
    }

    @Override
    public String toString() {
        return "ServiceBatchRequestImpl [groupKey=" + groupKey + ", batch=" + batch + "]";
    }
}