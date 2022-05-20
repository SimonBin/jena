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

package org.apache.jena.sparql.engine.main.iterator;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.aksw.commons.util.ref.RefFuture;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.ext.com.google.common.collect.Sets;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecException ;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.OpService ;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext ;
import org.apache.jena.sparql.engine.QueryIterator ;
import org.apache.jena.sparql.engine.Rename;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApplyBulk;
import org.apache.jena.sparql.engine.iterator.QueryIterSingleton;
import org.apache.jena.sparql.exec.http.QueryExecutionHTTP;
import org.apache.jena.sparql.exec.http.Service;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.service.BatchQueryRewriter;
import org.apache.jena.sparql.service.BatchQueryRewriter.BatchQueryRewriteResult;
import org.apache.jena.sparql.service.Finisher;
import org.apache.jena.sparql.service.PartitionIterator;
import org.apache.jena.sparql.service.QueryIterOverPartitionIter;
import org.apache.jena.sparql.service.QueryIterSlottedBase;
import org.apache.jena.sparql.service.ServiceCacheKey;
import org.apache.jena.sparql.service.ServiceCacheValue;
import org.apache.jena.sparql.service.ServiceExecution;
import org.apache.jena.sparql.service.ServiceExecutorFactory;
import org.apache.jena.sparql.service.ServiceExecutorRegistry;
import org.apache.jena.sparql.service.SimpleServiceCache;
import org.apache.jena.sparql.util.Context;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;


public class QueryIterServiceBulk extends QueryIterRepeatApplyBulk
{
	public static final int DEFAULT_BULK_SIZE = 30;
	public static final int DEFAULT_MAX_BYTE_SIZE = 5000;
	public static final Finisher DEFAULT_FINISHER =
			(partIt, execCxt) -> new QueryIterOverPartitionIter(partIt);

    protected OpService opService ;

    protected Query rawQuery; // Query for opService.getSubOp()
    protected Map<Var, Var> renames = new HashMap<>();

    protected Set<Var> serviceVars;

    // Maximum number of bindings to read from the input before being force to execute service requests
    protected int maxReadAhead;

    // If max read ahead is reached then ensure at least min read ahead items are scheduled for request
    protected int minScheduleAmount;


    // The index of the next binding that will be yeld by this iterator
    protected long currentIdx = 0;

    // The binding the needs to go to the next request
    protected Binding carryBinding = null;
    protected Node carryNode = null;
    protected Finisher finisher;

    protected SimpleServiceCache cache = new SimpleServiceCache();


    public QueryIterServiceBulk(QueryIterator input, OpService opService, ExecutionContext context)
    {
        super(input, context) ;
        if ( context.getContext().isFalse(Service.httpServiceAllowed) )
            throw new QueryExecException("SERVICE not allowed") ;
        // Old name.
        if ( context.getContext().isFalse(Service.serviceAllowed) )
            throw new QueryExecException("SERVICE not allowed") ;

        this.opService = opService ;
        // Get the variables used in the service clause (excluding the possible one for the service iri)
        Op subOp = opService.getSubOp();

        // Handling of a null supOp - can that happen?
        this.serviceVars = subOp == null ? Collections.emptySet() : new LinkedHashSet<>(OpVars.visibleVars(subOp));

        this.finisher = context.getContext().get(ARQ.serviceBulkRequestFinisher, DEFAULT_FINISHER);

        subOp = Rename.reverseVarRename(subOp, true);

        this.rawQuery = OpAsQuery.asQuery(subOp);
        // this.renames = new HashMap<>();

        VarExprList vel = rawQuery.getProject();
        VarExprList newVel = new VarExprList();

        int allocId = 0;
        for (Var var : vel.getVars()) {
        	Expr expr = vel.getExpr(var);
        	if (Var.isAllocVar(var)) {
        		Var tmp = Var.alloc("__v" + (++allocId) + "__");
        		renames.put(tmp, var);
        		var = tmp;
        	}
        	newVel.add(var, expr);
        }
        vel.clear();
        vel.addAll(newVel);
    }

    @Override
    protected QueryIterator nextStage(QueryIterator input) {

        boolean silent = opService.getSilent();
        ExecutionContext execCxt = getExecContext();
        Context cxt = execCxt.getContext();
        ServiceExecutorRegistry registry = ServiceExecutorRegistry.get(cxt);

    	int bulkSize = cxt.getInt(ARQ.serviceBulkRequestMaxItemCount, DEFAULT_BULK_SIZE);
    	int maxByteSize = cxt.getInt(ARQ.serviceBulkRequestMaxByteSize, DEFAULT_MAX_BYTE_SIZE);

        Op subOp = opService.getSubOp();


        int approxRequestSize = rawQuery.toString().getBytes(StandardCharsets.UTF_8).length;

    	Node serviceNode = opService.getService();

    	Var serviceVar = serviceNode.isVariable() ? (Var)serviceNode: null;
    	Binding[] bulk = new Binding[bulkSize];
    	Set<Var> seenVars = new HashSet<>();

    	int i = 0;
    	if (carryBinding != null) {
    		bulk[0] = carryBinding;
    	}


    	MultiServiceRequestMgr multiRequestMgr = new MultiServiceRequestMgr(opService);


    	// Retrieve bindings as long as the service node remains the same
    	// If there is a change then the binding is carries over to the next 'nextStage()' call
    	while (i < bulkSize && input.hasNext()) {
    		Binding binding = input.next();

    		Set<Var> bindingVars = Sets.newHashSet(binding.vars());
    		seenVars.addAll(bindingVars);

    		// b.vars().forEachRemaining(seenVars::add);

    		if (serviceVar != null) {
    			Node substServiceNode = binding.get(serviceVar);
    			if (carryNode != null) {
    				if (!Objects.equals(carryNode, substServiceNode)) {
    					carryNode = substServiceNode;
    					carryBinding = binding;
    					break;
    				}
    			} else {
    				carryNode = substServiceNode;
    			}
    		}


            ServiceCacheKey cacheKey = new ServiceCacheKey(serviceNode, subOp, binding);
    		Set<Var> joinVars = Sets.intersection(serviceVars, bindingVars);

            // Claim the cache entry - protects it from concurrent eviction
            // The ref must be closed eventually
            try (RefFuture<ServiceCacheValue> serviceCacheRef = cache.claim(cacheKey)) {
            	RangeSet<Long> fetchRanges = serviceCacheRef.await().getFetchRanges(Range.atLeast(0l));

            	// TODO We need to keep track of which ranges need to be fetched for which binding
            	// Conversely, we need to ensure to use the cache for available ranges of cached data
            }


    		bulk[i++] = binding;

			int contribSize = binding.toString().getBytes(StandardCharsets.UTF_8).length;
			approxRequestSize += contribSize;

			if (approxRequestSize > maxByteSize) {
				break;
			}
    	}

    	int n = i; // Set n to the number of available bindings



        // Table table = TableFactory.create(new ArrayList<>(joinVars));
        // bulkList.forEach(table::addBinding);

        // Convert to query so we can more easily set up the sort order condition



        // Fake the outer binding which is passed to the ServiceExector
    	// TODO Make ServiceExecutor API bulk request aware
        Binding outerBinding = BindingFactory.root();

        //Set<Var> joinVars = Sets.intersection(serviceVars, seenVars);





    	Var idxVar = Var.alloc("__idx__");
        BatchQueryRewriteResult rewrite = new BatchQueryRewriter(rawQuery, subOp, renames, idxVar, serviceVars)
        		.rewrite(bulk, n, seenVars);


        Op newSubOp = rewrite.getOp();
        // Map<Var, Var> renames = rewrite.renames;

        OpService substitutedOp = new OpService(serviceNode, newSubOp, silent);
        // OpService substitutedOp = (OpService)QC.substitute(opService, outerBinding);


        PartitionIterator partIt = execPartition(silent, execCxt, registry, bulkSize, bulk, outerBinding, idxVar, substitutedOp);

        QueryIterator result = finisher.finish(partIt, execCxt);


    }

	public  PartitionIterator execPartition(boolean silent, ExecutionContext execCxt, ServiceExecutorRegistry registry,
			int bulkSize, Binding[] bulk, Binding outerBinding, Var idxVar,
			OpService substitutedOp) {

        ServiceExecution svcExec = null;
		try {
            // ---- Find handler
            if ( registry != null ) {
                for ( ServiceExecutorFactory factory : registry.getFactories() ) {
                    // Internal consistency check
                    if ( factory == null ) {
                        Log.warn(this, "SERVICE <" + opService.getService().toString() + ">: Null item in custom ServiceExecutionRegistry");
                        continue;
                    }

                    svcExec = factory.createExecutor(substitutedOp, opService, outerBinding, execCxt);
                    if ( svcExec != null )
                        break;
                }
            }

            // ---- Execute
            if ( svcExec == null )
                throw new QueryExecException("No SERVICE handler");
            QueryIterator qIter = svcExec.exec();
            qIter = QueryIter.makeTracked(qIter, getExecContext());
            // Need to put the outerBinding as parent to every binding of the service call.
            // There should be no variables in common because of the OpSubstitute.substitute
            // return new QueryIterCommonParent(qIter, outerBinding, getExecContext());


            PartitionIterator result = new PartitionIterator(opService, serviceVars, qIter, idxVar, bulk, bulkSize, renames);

    		return result;

        } catch (RuntimeException ex) {
            if ( silent ) {
                Log.warn(this, "SERVICE " + NodeFmtLib.strTTL(substitutedOp.getService()) + " : " + ex.getMessage());
                // Return the input
                return QueryIterSingleton.create(input, getExecContext());

            }
            throw ex;
        }
	}

    public static void main(String[] args) {
    	Model model;

    	try (QueryExecution qe = QueryExecutionHTTP.newBuilder()
    		.endpoint("https://dbpedia.org/sparql")
    		.query("CONSTRUCT WHERE { ?s a <http://dbpedia.org/ontology/Person> } LIMIT 10")
    		.build()) {
    		model = qe.execConstruct();
    	}

    	//		"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { SELECT ?s (COUNT(*) AS ?c) { ?s ?p ?o } GROUP BY ?s } } }",

    	try (QueryExecution qe = QueryExecutionFactory.create(
        		"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { SELECT ?s ?p { ?s ?p ?o } ORDER BY ?p } } }",
    			model)) {
    		// qe.getContext().set(ARQ.serviceBulkRequestMaxItemCount, 1);
    		qe.getContext().set(ARQ.serviceBulkRequestMaxByteSize, 1500);
    		ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.RS_JSON);
        }

    	try (QueryExecution qe = QueryExecutionFactory.create(
        		"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { SELECT * { ?s ?p ?o } LIMIT 3 OFFSET 5 } } }",
    			model)) {
    		// qe.getContext().set(ARQ.serviceBulkRequestMaxItemCount, 1);
    		qe.getContext().set(ARQ.serviceBulkRequestMaxByteSize, 1500);
    		ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.RS_JSON);
        }

    }
}
