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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.service.enhancer.impl.util.QueryIterSlottedBase;

/**
 * Wrapper that reads {@link #batchSize} items at once from the underlying query iterator
 * into a list. Then the {@link #onBatch(List)} handler is notified and finally the items
 * of the list are emitted. The last call to handler will always be with an empty list.
 *
 * Intended use case is to cache items in bulk which may avoid excessive synchronization on every
 * single item.
 */
public class QueryIterWrapperBulk
    extends QueryIterSlottedBase<Binding>
{
    protected Iterator<Binding> activeBatchIt = Collections.<Binding>emptyList().iterator();
    protected int batchSize;
    protected QueryIterator delegate;

    public QueryIterWrapperBulk(QueryIterator qIter, int batchSize) {
        super();
        this.batchSize = batchSize;
        this.delegate = qIter;
    }

    @Override
    protected Binding moveToNext() {
        Binding result;
        if (!activeBatchIt.hasNext()) {
            List<Binding> newBatch = new ArrayList<>(batchSize);

            for (int i = 0; i < batchSize && delegate.hasNext(); ++i) {
                Binding binding = delegate.next();
                newBatch.add(binding);
            }

            // Notify about the next batch
            onBatch(newBatch);
            activeBatchIt = newBatch.iterator();

            result = activeBatchIt.hasNext() ? activeBatchIt.next() : null;
        } else {
            result = activeBatchIt.next();
        }
        return result;
    }

    @Override
    protected void closeIterator() {
        delegate.close();
    }

    protected void onBatch(List<Binding> batch) {}


}
