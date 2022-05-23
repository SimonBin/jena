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

import java.util.Iterator;

import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryExecException ;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.algebra.op.OpService ;
import org.apache.jena.sparql.engine.ExecutionContext ;
import org.apache.jena.sparql.engine.QueryIterator ;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApplyBulk;
import org.apache.jena.sparql.exec.http.QueryExecutionHTTP;
import org.apache.jena.sparql.exec.http.Service;
import org.apache.jena.sparql.service.OpServiceExecutorImpl;
import org.apache.jena.sparql.service.OpServiceInfo;
import org.apache.jena.sparql.service.RequestExecutor;
import org.apache.jena.sparql.service.RequestScheduler;
import org.apache.jena.sparql.service.ServiceBatchRequest;
import org.apache.jena.sparql.service.SimpleServiceCache;
import org.apache.jena.sparql.util.Context;


public class QueryIterServiceBulk extends QueryIterRepeatApplyBulk
{
	public static final int DEFAULT_BULK_SIZE = 30;
	public static final int DEFAULT_MAX_BYTE_SIZE = 5000;

	protected OpServiceInfo serviceInfo;

    // Maximum number of bindings to read from the input before being force to execute service requests
    // protected int maxReadAhead;

    // If max read ahead is reached then ensure at least min read ahead items are scheduled for request
    // protected int minScheduleAmount;

    protected SimpleServiceCache cache = new SimpleServiceCache();


    public QueryIterServiceBulk(QueryIterator input, OpService opService, ExecutionContext context)
    {
        super(input, context) ;
        if ( context.getContext().isFalse(Service.httpServiceAllowed) )
            throw new QueryExecException("SERVICE not allowed") ;
        // Old name.
        if ( context.getContext().isFalse(Service.serviceAllowed) )
            throw new QueryExecException("SERVICE not allowed") ;

        this.serviceInfo = new OpServiceInfo(opService);

    }

    @Override
    protected QueryIterator nextStage(QueryIterator input) {

    	ExecutionContext execCxt = getExecContext();
    	Context cxt = execCxt.getContext();

    	int bulkSize = cxt.getInt(ARQ.serviceBulkRequestMaxItemCount, DEFAULT_BULK_SIZE);

    	SimpleServiceCache serviceCache = cxt.get(ARQ.serviceCache);

    	OpServiceExecutorImpl opExecutor = new OpServiceExecutorImpl(serviceInfo.getOpService(), execCxt);

		RequestScheduler<Node, Binding> scheduler = new RequestScheduler<>(serviceInfo::getSubstServiceNode, bulkSize);
		Iterator<ServiceBatchRequest<Node, Binding>> inputBatchIterator = scheduler.group(input);
    	RequestExecutor exec = new RequestExecutor(opExecutor, serviceInfo, serviceCache, inputBatchIterator);

    	return exec;
    }


    public static void main(String[] args) {
    	Model model;

    	SimpleServiceCache serviceCache = new SimpleServiceCache();
    	ARQ.getContext().set(ARQ.serviceCache, serviceCache);


    	try (QueryExecution qe = QueryExecutionHTTP.newBuilder()
    		.endpoint("https://dbpedia.org/sparql")
    		.query("CONSTRUCT WHERE { ?s a <http://dbpedia.org/ontology/Person> } LIMIT 10")
    		.build()) {
    		model = qe.execConstruct();
    	}

    	//		"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { SELECT ?s (COUNT(*) AS ?c) { ?s ?p ?o } GROUP BY ?s } } }",

    	try (QueryExecution qe = QueryExecutionFactory.create(
        		"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { SELECT ?s ?p { ?s ?p ?o . FILTER(?p = <http://www.w3.org/2000/01/rdf-schema#label>) } ORDER BY ?p } } }",
    			model)) {
    		// qe.getContext().set(ARQ.serviceBulkRequestMaxItemCount, 1);
    		qe.getContext().set(ARQ.serviceBulkRequestMaxByteSize, 1500);
    		ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.RS_JSON);
        }


    	try (QueryExecution qe = QueryExecutionFactory.create(
        		"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { SELECT ?s ?p { ?s ?p ?o . FILTER(?p = <http://www.w3.org/2000/01/rdf-schema#label>) } ORDER BY ?p } } }",
    			model)) {
    		// qe.getContext().set(ARQ.serviceBulkRequestMaxItemCount, 1);
    		qe.getContext().set(ARQ.serviceBulkRequestMaxByteSize, 1500);
    		ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.RS_JSON);
        }

    	/*
    	try (QueryExecution qe = QueryExecutionFactory.create(
        		"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { SELECT * { ?s ?p ?o } LIMIT 3 OFFSET 5 } } }",
    			model)) {
    		// qe.getContext().set(ARQ.serviceBulkRequestMaxItemCount, 1);
    		qe.getContext().set(ARQ.serviceBulkRequestMaxByteSize, 1500);
    		ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.RS_JSON);
        }
        */

    }
}
