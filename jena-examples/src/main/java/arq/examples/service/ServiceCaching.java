package arq.examples.service;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.exec.http.QueryExecutionHTTP;
import org.apache.jena.sparql.service.ServiceExecution;
import org.apache.jena.sparql.service.ServiceExecutorFactory;
import org.apache.jena.sparql.service.ServiceExecutorRegistry;
import org.apache.jena.sparql.service.ServiceResponseCache;

/** Examples for setting up and using SERVICE caching */
public class ServiceCaching {

    public static void main(String[] args) {
    	Model model;

    	Node SELF = NodeFactory.createURI("urn:self");
    	ServiceExecutorFactory selfExec = (opExec, opOrig, binding, execCxt) -> {
    		ServiceExecution r = SELF.equals(opExec.getService())
    			? () -> {
    				return QC.execute(opExec.getSubOp(), binding, execCxt);
    			}
    			: null;
    			return r;
    	};
    	ServiceExecutorRegistry.get().getFactories().add(0, selfExec);


    	ServiceResponseCache serviceCache = new ServiceResponseCache();
    	ARQ.getContext().set(ARQ.serviceCache, serviceCache);


    	try (QueryExecution qe = QueryExecutionHTTP.newBuilder()
    		.endpoint("https://dbpedia.org/sparql")
    		.query("CONSTRUCT WHERE { ?s a <http://dbpedia.org/ontology/Person> } LIMIT 10")
    		.build()) {
    		model = qe.execConstruct();
    	}

    	// System.out.println(Algebra.compile(QueryFactory.create("SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { SELECT ?s ?p { ?s ?p ?o . FILTER(?p = <http://www.w3.org/2000/01/rdf-schema#label>) } ORDER BY ?p } } }")));
    	// System.out.println(Algebra.compile(QueryFactory.create("SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { BIND(?s AS ?x) } }")));

    	if (false) {
	    	try (QueryExecution qe = QueryExecutionFactory.create(
	    			//"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <http://dbpedia.org/sparql> { { SELECT * { { BIND(?s AS ?x) } UNION { BIND(?s AS ?y) } UNION { ?s <urn:dummy> ?s } } } } }",
	        		"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { SELECT ?x ?y { { BIND(?s AS ?x) } UNION { BIND(?s AS ?y) } } } } }",
	    			//"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { BIND(?s AS ?x) } UNION { BIND(?s AS ?y) } } }",
	    			model)) {
	    		 qe.getContext().set(ARQ.serviceBulkRequestMaxItemCount, 10);
	    		qe.getContext().set(ARQ.serviceBulkRequestMaxByteSize, 1500);
	    		ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.RS_JSON);
	        }
    	}

    	//		"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { SELECT ?s (COUNT(*) AS ?c) { ?s ?p ?o } GROUP BY ?s } } }",


    	if (true) {
	    	try (QueryExecution qe = QueryExecutionFactory.create(
	        		"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { SELECT ?s ?p { ?s ?p ?o . FILTER(?p = <http://www.w3.org/2000/01/rdf-schema#label>) } ORDER BY ?p } } }",
	    			model)) {
	    		 qe.getContext().set(ARQ.serviceBulkRequestMaxItemCount, 10);
	    		qe.getContext().set(ARQ.serviceBulkRequestMaxByteSize, 1500);
	    		ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.RS_JSON);
	        }
    	}

    	if (true) {
	    	try (QueryExecution qe = QueryExecutionFactory.create(
	        		"SELECT * { { SELECT ?s { ?s a <http://dbpedia.org/ontology/Person> } OFFSET 1 LIMIT 3 } SERVICE <https://dbpedia.org/sparql> { { SELECT ?s ?p { ?s ?p ?o . FILTER(?p = <http://www.w3.org/2000/01/rdf-schema#label>) } ORDER BY ?p } } }",
	    			model)) {
	    		 qe.getContext().set(ARQ.serviceBulkRequestMaxItemCount, 10);
	    		qe.getContext().set(ARQ.serviceBulkRequestMaxByteSize, 1500);
	    		ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.RS_JSON);
	        }
    	}

    	if (false) {
	    	try (QueryExecution qe = QueryExecutionFactory.create(
	        		"SELECT * { ?s a <http://dbpedia.org/ontology/Person> SERVICE <https://dbpedia.org/sparql> { { SELECT * { ?s ?p ?o } LIMIT 3 OFFSET 5 } } }",
	    			model)) {
	    		// qe.getContext().set(ARQ.serviceBulkRequestMaxItemCount, 1);
	    		qe.getContext().set(ARQ.serviceBulkRequestMaxByteSize, 1500);
	    		ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.RS_JSON);
	        }
    	}

    }
}
