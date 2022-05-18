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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.jena.atlas.logging.Log;
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
import org.apache.jena.sparql.service.ServiceCacheKey;
import org.apache.jena.sparql.service.ServiceExecution;
import org.apache.jena.sparql.service.ServiceExecutorFactory;
import org.apache.jena.sparql.service.ServiceExecutorRegistry;
import org.apache.jena.sparql.util.Context;

public class QueryIterServiceBulk extends QueryIterRepeatApplyBulk
{
	public static final int DEFAULT_BULK_SIZE = 30;
	public static final int DEFAULT_MAX_BYTE_SIZE = 5000;
	public static final Finisher DEFAULT_FINISHER =
			(partIt, execCxt) -> new QueryIterOverPartitionIter(partIt);

    protected OpService opService ;
    protected Set<Var> serviceVars;

    // The binding the needs to go to the next request
    protected Binding carryBinding = null;
    protected Node carryNode = null;
    protected Finisher finisher;


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
    }

    @Override
    protected QueryIterator nextStage(QueryIterator input) {

        boolean silent = opService.getSilent();
        ExecutionContext execCxt = getExecContext();
        Context cxt = execCxt.getContext();
        ServiceExecutorRegistry registry = ServiceExecutorRegistry.get(cxt);
        ServiceExecution svcExec = null;

    	int bulkSize = cxt.getInt(ARQ.serviceBulkRequestMaxItemCount, DEFAULT_BULK_SIZE);
    	int maxByteSize = cxt.getInt(ARQ.serviceBulkRequestMaxByteSize, DEFAULT_MAX_BYTE_SIZE);

        Op subOp = opService.getSubOp();
        subOp = Rename.reverseVarRename(subOp, true);


        Query rawQuery = OpAsQuery.asQuery(subOp);
        Map<Var, Var> renames = new HashMap<>();

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


        int approxRequestSize = rawQuery.toString().getBytes(StandardCharsets.UTF_8).length;

    	Node serviceNode = opService.getService();

    	Var serviceVar = serviceNode.isVariable() ? (Var)serviceNode: null;
    	Binding[] bulk = new Binding[bulkSize];
    	Set<Var> seenVars = new HashSet<>();

    	int i = 0;
    	if (carryBinding != null) {
    		bulk[0] = carryBinding;
    	}

    	// Retrieve bindings as long as the service node remains the same
    	// If there is a change then the binding is carries over to the next 'nextStage()' call
    	while (i < bulkSize && input.hasNext()) {
    		Binding b = input.next();

    		b.vars().forEachRemaining(seenVars::add);

    		if (serviceVar != null) {
    			Node substServiceNode = b.get(serviceVar);
    			if (carryNode != null) {
    				if (!Objects.equals(carryNode, substServiceNode)) {
    					carryNode = substServiceNode;
    					carryBinding = b;
    					break;
    				}
    			} else {
    				carryNode = substServiceNode;
    			}
    		}

    		bulk[i++] = b;

			int contribSize = b.toString().getBytes(StandardCharsets.UTF_8).length;
			approxRequestSize += contribSize;

			if (approxRequestSize > maxByteSize) {
				break;
			}
    	}

    	int n = i; // Set n to the number of available bindings



        // Table table = TableFactory.create(new ArrayList<>(joinVars));
        // bulkList.forEach(table::addBinding);

        // Convert to query so we can more easily set up the sort order condition



        // Fake the outer binding
        Binding outerBinding = BindingFactory.root();


    	Var idxVar = Var.alloc("__idx__");
        BatchQueryRewriteResult rewrite = new BatchQueryRewriter(rawQuery, subOp, renames, idxVar, serviceVars)
        		.rewrite(bulk, n, seenVars);

        // ServiceCacheKey cacheKey = new ServiceCacheKey(serviceNode, subOp, rewrite.getJoinVars());

        Op newSubOp = rewrite.getOp();
        // Map<Var, Var> renames = rewrite.renames;

        OpService substitutedOp = new OpService(serviceNode, newSubOp, silent);
        // OpService substitutedOp = (OpService)QC.substitute(opService, outerBinding);


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


            PartitionIterator partIt = new PartitionIterator(opService, serviceVars, qIter, idxVar, bulk, bulkSize, renames);


            QueryIterator result = finisher.finish(partIt, execCxt);


    		return result;

        } catch (RuntimeException ex) {
            if ( silent ) {
                Log.warn(this, "SERVICE " + NodeFmtLib.strTTL(substitutedOp.getService()) + " : " + ex.getMessage());
                // Return the input
                return QueryIterSingleton.create(outerBinding, getExecContext());

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
