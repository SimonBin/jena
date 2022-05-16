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

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.query.QueryExecException;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterSingleton;
import org.apache.jena.sparql.service.bulk.ServiceExecutorBulk;
import org.apache.jena.sparql.util.Context;

/** Helper class to simplify executing concrete OpService instances */
public class OpServiceExecutorImpl
    implements OpServiceExecutor
{
    protected OpService originalOp;

    // Cached attributes
    protected boolean silent;
    protected ExecutionContext execCxt;
    protected Context cxt;
    // protected ServiceExecutorRegistry registry;
    protected ServiceExecutorBulk delegate;

    public OpServiceExecutorImpl(OpService opService, ExecutionContext execCxt, ServiceExecutorBulk delegate) {
        this.originalOp = opService;
        this.silent = opService.getSilent();
        this.execCxt = execCxt;
        this.cxt = execCxt.getContext();
        // this.registry = ServiceExecutorRegistry.get(cxt);
        this.delegate = delegate;
    }

    public ExecutionContext getExecCxt() {
        return execCxt;
    }

    public QueryIterator exec(OpService substitutedOp) {

        Binding input = BindingFactory.binding();

        QueryIterator svcExec = null;
        try {
            // ---- Find handler
            // if ( registry != null ) {
            //	ServiceExecutorFactoryChain chain = new ServiceExecutorFactoryChainOverRegistry(registry);

                QueryIterator singleton = QueryIterSingleton.create(BindingFactory.root(), execCxt);
                svcExec = delegate.createExecution(substitutedOp, singleton, execCxt);

//                for ( ServiceExecutorFactory factory : registry.getFactories() ) {
//                    // Internal consistency check
//                    if ( factory == null ) {
//                        Log.warn(this, "SERVICE <" + substitutedOp.getService().toString() + ">: Null item in custom ServiceExecutionRegistry");
//                        continue;
//                    }
//
//                    svcExec = factory.createExecutor(substitutedOp, substitutedOp, input, execCxt);
//                    if ( svcExec != null )
//                        break;
//                }
            // }

            // ---- Execute
            if ( svcExec == null )
                throw new QueryExecException("No SERVICE handler");
            QueryIterator qIter = svcExec;
            qIter = QueryIter.makeTracked(qIter, execCxt);
            // Need to put the outerBinding as parent to every binding of the service call.
            // There should be no variables in common because of the OpSubstitute.substitute
            // return new QueryIterCommonParent(qIter, outerBinding, getExecContext());

            return qIter;

            //PartitionIterator result = new PartitionIterator(opService, serviceVars, qIter, idxVar, bulk, bulkSize, renames);

        } catch (RuntimeException ex) {
            if ( silent ) {
                Log.warn(this, "SERVICE " + NodeFmtLib.strTTL(substitutedOp.getService()) + " : " + ex.getMessage());
                // Return the input
                return QueryIterSingleton.create(input, execCxt);

            }
            throw ex;
        }
    }
}
