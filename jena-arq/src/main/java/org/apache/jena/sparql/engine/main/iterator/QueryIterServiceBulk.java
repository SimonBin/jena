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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecException ;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.SortCondition;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.OpService ;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext ;
import org.apache.jena.sparql.engine.QueryIterator ;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingProject;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApplyBulk;
import org.apache.jena.sparql.engine.iterator.QueryIterSingleton;
import org.apache.jena.sparql.engine.iterator.QueryIteratorWrapper;
import org.apache.jena.sparql.exec.http.QueryExecutionHTTP;
import org.apache.jena.sparql.exec.http.Service;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.service.ServiceExecution;
import org.apache.jena.sparql.service.ServiceExecutorFactory;
import org.apache.jena.sparql.service.ServiceExecutorRegistry;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementData;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementSubQuery;
import org.apache.jena.sparql.util.Context;


public class QueryIterServiceBulk extends QueryIterRepeatApplyBulk
{
    protected OpService opService ;
    protected Set<Var> serviceVars;

    // The binding the needs to go to the next request
    protected Binding carryBinding = null;
    protected Node carryNode = null;

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
    }

    @Override
    protected QueryIterator nextStage(QueryIterator input) {

    	int n = 30;

    	Node serviceNode = opService.getService();

    	Var serviceVar = serviceNode.isVariable() ? (Var)serviceNode: null;
    	Binding[] bulk = new Binding[n];
    	Set<Var> seenVars = new HashSet<>();

    	int i = 0;
    	if (carryBinding != null) {
    		bulk[0] = carryBinding;
    	}

    	// Retrieve bindings as long as the service node remains the same
    	for (; i < n && input.hasNext(); ++i) {
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

    		bulk[i] = b;
    	}

    	n = i; // Set n to the number of available bindings


    	Set<Var> joinVars = new LinkedHashSet<>(serviceVars);
    	joinVars.retainAll(seenVars);


    	Var idxVar = Var.alloc("__idx__");

    	// Project the bindings to those variables that are also visible
    	// in the service cause
    	for (i = 0; i < n; ++i) {
    		bulk[i] = BindingFactory.binding(
    				new BindingProject(joinVars, bulk[i]),
    				idxVar, NodeValue.makeInteger(i).asNode());
    	}
        List<Binding> bulkList = Arrays.asList(bulk).subList(0, n);

        // Table table = TableFactory.create(new ArrayList<>(joinVars));
        // bulkList.forEach(table::addBinding);

        Op subOp = opService.getSubOp();
        // Convert to query so we can more easily set up the sort order condition
        Query rawQuery = OpAsQuery.asQuery(subOp);
        Query q;

        boolean wrapAsSubQuery = needsWrappingByFeatures(rawQuery);
        if (wrapAsSubQuery) {
        	q = new Query();
        	q.setQuerySelectType();
        	q.setQueryPattern(new ElementSubQuery(rawQuery));
        	if (rawQuery.hasOrderBy()) {
        		q.getOrderBy().addAll(rawQuery.getOrderBy());
        	}
        	rawQuery.getOrderBy().clear();
        } else {
        	q = rawQuery;
        }

        boolean injectIdx = true;

        if (injectIdx) {
        	SortCondition sc = new SortCondition(new ExprVar(idxVar), Query.ORDER_ASCENDING);
        	if (q.hasOrderBy()) {
        		q.getOrderBy().add(0, sc);
        	} else {
        		q.addOrderBy(sc);
        	}
        }

        q.resetResultVars();
        q.setQueryResultStar(false);
        q.getProjectVars().removeAll(joinVars);
        q.getProjectVars().add(0, idxVar);
        q.resetResultVars();

        List<Var> remoteVars = new ArrayList<>(1 + joinVars.size());
        remoteVars.add(idxVar);
        remoteVars.addAll(joinVars);
        ElementData dataBlock = new ElementData(remoteVars, bulkList);
        Element before = q.getQueryPattern();
        ElementGroup after = new ElementGroup();
    	after.addElement(dataBlock);
        if (before instanceof ElementGroup) {
        	((ElementGroup)before).getElements().forEach(after::addElement);
        } else {
        	after.addElement(before);
        }
        q.setQueryPattern(after);

System.out.println(q);

        Op newSubOp = Algebra.compile(q);
        // Op newSubOp = OpJoin.create(OpTable.create(table), subOp);



        // Fake the outer binding
        Binding outerBinding = BindingFactory.root();


        boolean silent = opService.getSilent();
        ExecutionContext execCxt = getExecContext();
        Context cxt = execCxt.getContext();
        ServiceExecutorRegistry registry = ServiceExecutorRegistry.get(cxt);
        ServiceExecution svcExec = null;

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


            QueryIterator result = new QueryIteratorWrapper(qIter) {
    			@Override
    			protected Binding moveToNextBinding() {
    				Binding child = super.moveToNextBinding();

    				Node idxNode = child.get(idxVar);
    				Object obj = idxNode.getLiteralValue();
    				if (!(obj instanceof Number)) {
    					throw new ExprEvalException("Index was not returned as a number");
    				}
    				int idx = ((Number)obj).intValue();
    				Binding parent = bulk[idx];

    				Binding result = BindingFactory.builder(parent).addAll(child).build();

    				return result;
    			}
    		};

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



    /**
     * Returns true if the query uses features that prevents it from being
     * represented as a pair of graph pattern + projection
     *
     * @param query
     * @return
     */
    public static boolean needsWrappingByFeatures(Query query) {
        return needsWrappingByFeatures(query, true) || !query.getProject().getExprs().isEmpty();
    }


    /**
     * Similar to {@link #needsWrapping(Query)} but includes a flag
     * whether to include slice information (limit / offset).
     *
     * @param query
     * @return
     */
    public static boolean needsWrappingByFeatures(Query query, boolean includeSlice) {
        boolean result
             = query.hasGroupBy()
            || query.hasAggregators()
            || query.hasHaving()
            || query.hasValues();

        if (includeSlice) {
            result = result
                || query.hasLimit()
                || query.hasOffset()
                ;
        }

        // Order is ignored

        return result;
    }

    public static void main(String[] args) {
    	Model model;

    	try (QueryExecution qe = QueryExecutionHTTP.newBuilder()
    		.endpoint("http://dbpedia.org/sparql")
    		.query("CONSTRUCT WHERE { ?s a <http://dbpedia.org/ontology/Person> } LIMIT 10")
    		.build()) {
    		model = qe.execConstruct();
    	}

    	try (QueryExecution qe = QueryExecutionFactory.create(
        		"SELECT * { ?s a <http://dbpedia.org/ontology/Person>  SERVICE <http://dbpedia.org/sparql> { ?s ?p ?o } }",
    			model)) {
    		ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.RS_JSON);
        }

    }
}
