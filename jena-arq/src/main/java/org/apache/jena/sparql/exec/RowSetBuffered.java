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

package org.apache.jena.sparql.exec;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.apache.jena.atlas.data.DataBag;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

/**
 * A buffering RowSet wrapper for stream-backed RowSets whose
 * {@link #getResultVars()} returns null as long as the header has not been
 * seen on the stream.
 *
 * Calling {@link #getResultVars()} buffers bindings from the delegate RowSet
 * until it returns a non-null value for its result vars.
 * The buffered bindings are replayed on this instance in their appropriate
 * order.
 *
 */
public class RowSetBuffered
    extends AbstractIterator<Binding>
    implements RowSet
{
    protected RowSet delegate;

    // The buffer may be filled upon calling getResultVars()
    // Data will be served from the buffer first until it is exhausted, then
    // data is served from the delegate again

    protected Supplier<DataBag<Binding>> bufferFactory;
    protected DataBag<Binding> buffer = null;
    protected Iterator<Binding> bufferIterator = null;

    protected long rowNumber;

    public RowSetBuffered(RowSet delegate, Supplier<DataBag<Binding>> bufferFactory) {
        this(delegate, bufferFactory, 0);
    }

    public RowSetBuffered(RowSet delegate, Supplier<DataBag<Binding>> bufferFactory, long rowNumber) {
        super();
        this.delegate = delegate;
        this.bufferFactory = bufferFactory;
        this.rowNumber = rowNumber;
    }

    public RowSet getDelegate() {
        return delegate;
    }

    /** Reads and buffers bindings until the delegate's header no longer returns null */
    @Override
    public List<Var> getResultVars() {
        List<Var> result = getDelegate().getResultVars();

        if (result == null && getDelegate().hasNext()) {
            if (buffer == null) {
                buffer = bufferFactory.get();
            }

            while (((result = getDelegate().getResultVars()) == null) && getDelegate().hasNext()) {
                Binding b = getDelegate().next();
                buffer.add(b);

                // Log a warning if we read a lot of data here?
            }
        }

        return result;
    }

    @Override
    public long getRowNumber() {
        return rowNumber;
    }

    @Override
    public void close() {
        try {
            if (buffer != null) {
                buffer.close();
            }
        } finally {
            getDelegate().close();
        }
    }

    @Override
    protected Binding computeNext() {
        Binding result;

        if (bufferIterator != null) {
            if (bufferIterator.hasNext()) {
                result = bufferIterator.next();
            } else {
                buffer.close();
                buffer = null;
                bufferIterator = null;
                result = nextFromDelegate();
            }
        } else {
            result = nextFromDelegate();
        }

        ++rowNumber;
        return result;
    }

    protected Binding nextFromDelegate() {
        Binding result = getDelegate().hasNext()
            ? getDelegate().next()
            : endOfData();
        return result;
    }

}
