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

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.optimize.Optimize;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.service.enhancer.algebra.TransformSE_JoinStrategy;
import org.apache.jena.sparql.service.enhancer.init.InitServiceEnhancer;
import org.junit.Assert;
import org.junit.Test;

public class TestServiceEnhancer {

    @Test
    public void testLargeCache01() {
        String queryStr = String.join("\n",
                "SELECT * {",
                "  SERVICE <cache:> { ?s ?p ?o }",
                "}");

        Model model = TS_ResultSetLimits.createModel(1000);
        int evalRowCount = TS_ResultSetLimits.testWithCleanCaches(model, queryStr, 1000000000);
        int cachedRowCount = TS_ResultSetLimits.testCore(model, queryStr, 1000000000);

        Assert.assertEquals(evalRowCount, cachedRowCount);
    }

    /** A query where it's whole graph pattern is subject to caching */
    @Test
    public void testCacheFullQuery() {
        // TODO We need to clean up caches after testing!
        ServiceResultSizeCache.get().invalidateAll();
        ServiceResponseCache.get().invalidateAll();

        Model model = TS_ResultSetLimits.createModel(1000);
        int rows = TS_ResultSetLimits.testCore(model, "SELECT * { SERVICE <cache:> { SELECT DISTINCT ?p { ?s ?p ?o } } }", 100);
        Assert.assertEquals(3, rows);

        // TODO We need to check that no backend request is made
        // We could register a custom service executor that does the counting
        // And/Or the rest runner return a stats object which includes the number of backend requests

        int cachedRows = TS_ResultSetLimits.testCore(model, "SELECT * { SERVICE <cache:> { SELECT DISTINCT ?p { ?s ?p ?o } } }", 100);
        Assert.assertEquals(3, cachedRows);
    }

    @Test
    public void testNestedLoopWithPropertyFunction() {
        String queryStr = String.join("\n",
                "PREFIX apf: <http://jena.apache.org/ARQ/property#>",
                "SELECT * {",
                "  SERVICE <loop:> { ?x1 ?p1 ?x2 }",
                "  SERVICE <loop:> { ?x2 ?p2 ?x3 }",
                "  SERVICE <loop:> { ?x3 apf:assign ?x4 }",
                "}");

        Model model = TS_ResultSetLimits.createModel(10);
        int rows = TS_ResultSetLimits.testWithCleanCaches(model, queryStr, 1000);
        Assert.assertEquals(110, rows);
    }

    /** Tests that a loop join where the scoped visible variables on either side are disjoint
     *  results in the right substitution */
    @Test
    public void testLoopJoinWithScope() {
        String queryStr = String.join("\n",
                "SELECT * {",
                "  { SELECT DISTINCT ?s { ?s a <urn:Department> ; ?p ?o } ORDER BY ?s }",
                "  SERVICE <loop:> { SELECT ?o { ?s <urn:hasEmployee> ?o } ORDER BY DESC(?o) LIMIT 1 }",
                "}");

        Model model = TS_ResultSetLimits.createModel(9);
        int rows = TS_ResultSetLimits.testWithCleanCaches(model, queryStr, 1000);
        Assert.assertEquals(9, rows);
    }

    @Test
    public void testLookupJoinWithScopeAndCache() {
        String queryStr = String.join("\n",
                "SELECT * {",
                "  { SELECT DISTINCT ?s { ?s a <urn:Department> ; ?p ?o } ORDER BY ?s }",
                "  SERVICE <cache:loop:> { SELECT ?o { ?s <urn:hasEmployee> ?o } ORDER BY DESC(?o) LIMIT 1 }",
                "}");

        Model model = TS_ResultSetLimits.createModel(9);
        int expectedRowCount = TS_ResultSetLimits.testWithCleanCaches(model, queryStr, 1000);
        int actualRowCount = TS_ResultSetLimits.testCore(model, queryStr, 1000);
        Assert.assertEquals(expectedRowCount, actualRowCount);
        // Assert.assertEquals(9, rows);

/*
-------------------------------
| s           | o             |
===============================
| <urn:dept1> | <urn:person9> |
| <urn:dept2> | <urn:person8> |
| <urn:dept3> | <urn:person7> |
 */
    }

    @Test
    public void testStdJoinWithScope() {
        String queryStr = String.join("\n",
                "SELECT * {",
                "  { SELECT DISTINCT ?s { ?s a <urn:Department> ; ?p ?o } ORDER BY ?s }",
                "  SERVICE <urn:x-arq:self> { SELECT ?o { ?s <urn:hasEmployee> ?o } ORDER BY DESC(?o) LIMIT 1 }",
                "}");

        /*
-------------------------------
| s           | o             |
===============================
| <urn:dept1> | <urn:person9> |
| <urn:dept2> | <urn:person9> |
| <urn:dept3> | <urn:person9> |
         */

        Model model = TS_ResultSetLimits.createModel(9);
        int actualRowCount = TS_ResultSetLimits.testWithCleanCaches(model, queryStr, 1000);
        Assert.assertEquals(9, actualRowCount);
    }

    @Test
    public void testNestedCache() {
        String queryStr = String.join("\n",
                "SELECT * {",
                "  SERVICE <cache:> {",
                "    SERVICE <xcache:> { SELECT ?s { ?s a <urn:Department> } ORDER BY ?s OFFSET 7 LIMIT 2 }",
                "    SERVICE <loop:xcache:> { ?s <urn:hasEmployee> ?o }",
                "  }",
                "}").replace("xcache:", "urn:x-arq:self");

        Model model = TS_ResultSetLimits.createModel(9);
        int referenceRowCount = TS_ResultSetLimits.testWithCleanCaches(model, queryStr, 1000);
        int actualRowCount = TS_ResultSetLimits.testCore(model, queryStr, 1000);

        Assert.assertEquals(3, referenceRowCount);
        Assert.assertEquals(referenceRowCount, actualRowCount);
    }

    @Test
    public void testCacheRefresh() {
        String queryStr = String.join("\n",
                "SELECT * {",
                "  SERVICE <cache+clear:> {",
                "    SERVICE <cache+clear:> { SELECT ?s { ?s a <urn:Department> } ORDER BY ?s OFFSET 7 LIMIT 2 }",
                "    SERVICE <loop:cache+clear:> { ?s <urn:hasEmployee> ?o }",
                "  }",
                "}"); // .replace("xcache:", "urn:x-arq:self");

        Model model = TS_ResultSetLimits.createModel(9);
        int referenceRowCount = TS_ResultSetLimits.testWithCleanCaches(model, queryStr, 1000);
        int actualRowCount = TS_ResultSetLimits.testCore(model, queryStr, 1000);

        Assert.assertEquals(3, referenceRowCount);
        Assert.assertEquals(referenceRowCount, actualRowCount);
    }

    @Test
    public void testCacheRefreshWithOffsetOutside() {
        String queryStr = String.join("\n",
                "SELECT * {",
                "  SERVICE <cache:> {",
                "    SELECT ?s { ?s a <urn:Department> } ORDER BY ?s",
                "  }",
                "} LIMIT 5 OFFSET 5"); // .replace("xcache:", "urn:x-arq:self");

        Model model = TS_ResultSetLimits.createModel(9);
        int referenceRowCount = TS_ResultSetLimits.testWithCleanCaches(model, queryStr, 1000);

        queryStr = queryStr.replace("cache:", "cache+clear:");
        int actualRowCount = TS_ResultSetLimits.testCore(model, queryStr, 1000);

        Assert.assertEquals(4, referenceRowCount);
        Assert.assertEquals(referenceRowCount, actualRowCount);;
    }

    @Test
    public void testCacheRefreshWithOffsetInside() {
        String queryStr = String.join("\n",
                "SELECT * {",
                "  SERVICE <cache:> {",
                "    SELECT ?s { ?s a <urn:Department> } ORDER BY ?s OFFSET 5 LIMIT 10",
                "  }",
                "}"); // .replace("xcache:", "urn:x-arq:self");

        Model model = TS_ResultSetLimits.createModel(9);
        int rows = TS_ResultSetLimits.testWithCleanCaches(model, queryStr, 1000);
        int rows2 = TS_ResultSetLimits.testCore(model, queryStr, 1000);

        queryStr = queryStr.replace("cache:", "cache+clear:");
        int rows3 = TS_ResultSetLimits.testCore(model, queryStr, 1000);

        Assert.assertEquals(4, rows);
        Assert.assertEquals(rows, rows2);
        Assert.assertEquals(rows, rows3);
    }

    @Test
    public void testSubstitute() {
        String queryStr = String.join("\n",
                "SELECT ?s {",
                "  ?s ?p ?o ",
                "  { SELECT ?p {",
                "    ?s ?p ?o ",
                "  } }",
                "}");

        String expectedQueryStr = String.join("\n",
                "SELECT  ?s",
                "WHERE",
                "  { <urn:s>  ?p  ?o",
                "    { SELECT  ?p",
                "      WHERE",
                "        { <urn:s>  ?p  ?o }",
                "    }",
                "  }");

        Query expectedQuery = QueryFactory.create(expectedQueryStr);
        Op op = Algebra.compile(QueryFactory.create(queryStr));
        Op op2 = Substitute.substitute(op, BindingFactory.binding(Var.alloc("s"), NodeFactory.createURI("urn:s")));
        Query actualQuery = OpAsQuery.asQuery(op2);
        Assert.assertEquals(expectedQuery, actualQuery);
    }

    /** Tests for the presence of the function cacheInvalidate and expects it to return one binding
     * with the number of invalidated entries */
    @Test
    public void testCacheMgmtInvalidate() {
        String queryStr = String.join("\n",
                "PREFIX sefn: <java:org.apache.jena.sparql.service.enhancer.function.>",
                "SELECT (sefn:cacheInvalidate() AS ?count) WHERE {",
                "}");

        int actualRowCount = TS_ResultSetLimits.testWithCleanCaches(ModelFactory.createDefaultModel(), queryStr, 1000);
        Assert.assertEquals(1, actualRowCount);
    }

    /** Tests for the presence of the property function cacheList */
    @Test
    public void testCacheMgmtList() {

        // This call creates one cache entry
        testCacheRefreshWithOffsetInside();

        String queryStr = String.join("\n",
                "PREFIX sepf: <java:org.apache.jena.sparql.service.enhancer.pfunction.>",
                "SELECT * WHERE {",
                "  ?id sepf:cacheList (?op ?binding ?start ?end)",
                "}");

        int actualRowCount = TS_ResultSetLimits.testCore(ModelFactory.createDefaultModel(), queryStr, 1000);
        Assert.assertEquals(1, actualRowCount);
    }

    @Test
    public void testWikiData() {
        String queryStr = String.join("\n",
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
                "PREFIX wd: <http://www.wikidata.org/entity/>",
                "SELECT * { SERVICE <cache:> {",
            "SELECT ?s ?l {",
                "  # Apache Jena, Semantic Web, RDF, SPARQL, Andy Seaborne",
                "  VALUES ?s { wd:Q1686799 wd:Q54837 wd:Q54872 wd:Q54871 wd:Q108379795 }",
                "  SERVICE <cache:loop:bulk+5:https://query.wikidata.org/sparql> {",
                "    SELECT ?l {",
                "      ?s rdfs:label ?l",
                "      FILTER(langMatches(lang(?l), 'en'))",
                "    } ORDER BY ?l LIMIT 1",
                "  }",
                "}",
            "} }");

        int referenceRowCount = TS_ResultSetLimits.testWithCleanCaches(ModelFactory.createDefaultModel(), queryStr, 1000);
        int actualRowCount = TS_ResultSetLimits.testCore(ModelFactory.createDefaultModel(), queryStr, 1000);
        Assert.assertEquals(referenceRowCount, actualRowCount);
    }

    @Test
    public void testSubSelectInService() {
        String queryStr = String.join("\n",
                "SELECT ?s ?o {",
                "  { SELECT ?s { ?s a <urn:Department> } ORDER BY ?s OFFSET 6 LIMIT 3 } ",
                "  SERVICE <cache:loop:> {",
                "    SELECT ?o {",
                "    ?s <urn:hasEmployee> ?o",
                "  } ORDER BY ?o }",
                "}");

         Model model = TS_ResultSetLimits.createModel(9);
         int actualRowCount = TS_ResultSetLimits.testWithCleanCaches(model, queryStr, 1000);

         Assert.assertEquals(6, actualRowCount);
    }

    @Test
    public void testLoopScope() {
        String queryStr = String.join("\n",
                "SELECT ?s {",
                "  ?s ?p ?o ",
                "  { SELECT ?p {",
                "    ?s ?p ?o ",
                // "    SERVICE <loop:> { SELECT ?x { ?o ?x ?y } LIMIT 1 }",
                "    SERVICE <loop:> { SELECT ?x { ?o ?x ?y } }",
                "  } }",
                "}");

        String expectedStr = String.join("\n",
                "(project (?s)",
                "  (sequence",
                "    (bgp (triple ?s ?p ?o))",
                "    (project (?p)",
                "      (sequence",
                "        (bgp (triple ?/s ?p ?/o))",
                "        (service <loop:>",
                "          (project (?/x)",
                "            (bgp (triple ?/o ?/x ?//y))))))))",
                "");

         Op op = Algebra.compile(QueryFactory.create(queryStr));
         Op op2 = Optimize.stdOptimizationFactory.create(ARQ.getContext()).rewrite(op);
         Op op3 = Transformer.transform(new TransformSE_JoinStrategy(), op2);

         Assert.assertEquals(expectedStr, op3.toString());
    }

    @Test
    public void testScope3() {
        Dataset dataset = DatasetFactory.create();
        dataset.getNamedModel("urn:g1").add(TS_ResultSetLimits.createModel(5));
        dataset.getNamedModel("urn:g2").add(TS_ResultSetLimits.createModel(4));
        dataset.getNamedModel("urn:g3").add(TS_ResultSetLimits.createModel(3));

        String queryStr = String.join("\n",
           "SELECT ?c {",
                    "SELECT ?c WHERE {\n"
                     // + "  VALUES ?p {geo:hasGeometry}\n"
                     + "    BIND(<urn:hasEmployee> AS ?p)\n"
                   + "SERVICE <loop:> {"
                    + "    { SERVICE <bulk+3:cache:> { SELECT ?g (count(*) AS ?c) { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g } }\n"
                   + "}"
                    + "}",

             "}"); //.replace("loop:", "urn:x-arq:self");

        int referenceRowCount = TS_ResultSetLimits.testWithCleanCaches(dataset, queryStr, 1000);
        Assert.assertEquals(3, referenceRowCount);
        int actualRowCount = TS_ResultSetLimits.testCore(dataset, queryStr, 1000);
        Assert.assertEquals(referenceRowCount, actualRowCount);
    }

    @Test
    public void testScopeSimple() {
        String queryStr2 = String.join("\n",
                "SELECT ?p ?c {",
                "  BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> AS ?p)",
                "  { SELECT (COUNT(*) AS ?c) { ?s ?p ?o } }",
                "}");

        String queryStr = String.join("\n",
                "SELECT ?p ?c {",
                "  BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> AS ?p)",
                "  SERVICE <loop:> { SELECT (COUNT(*) AS ?c) { ?s ?p ?o } }",
                "}");

        String expectedStr = String.join("\n",
                "(project (?p ?c)",
                "  (sequence",
                "    (extend ((?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>))",
                "      (table unit))",
                "    (service <loop:>",
                "      (project (?c)",
                "        (extend ((?c ?/.0))",
                "          (group () ((?/.0 (count)))",
                "            (bgp (triple ?/s ?p ?/o))))))))",
                "");


        Transform loopTransform = new TransformSE_JoinStrategy();
        Op op0 = Algebra.compile(QueryFactory.create(queryStr));
        Op op1 = Transformer.transform(loopTransform, op0);
        Op op2 = Optimize.stdOptimizationFactory.create(ARQ.getContext()).rewrite(op1);
        Op op3 = Transformer.transform(loopTransform, op2);

        Assert.assertEquals(expectedStr, op3.toString());
    }

    @Test
    public void testNormalization01() {
        Dataset dataset = DatasetFactory.create();
        dataset.getNamedModel("urn:g1").add(TS_ResultSetLimits.createModel(5));
        dataset.getNamedModel("urn:g2").add(TS_ResultSetLimits.createModel(4));
        dataset.getNamedModel("urn:g3").add(TS_ResultSetLimits.createModel(3));

        String queryStr = "SELECT * WHERE {\n"
                + "    { SERVICE <cache:> { SELECT ?g (count(*) AS ?c) { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g } }\n"
                + "  UNION\n"
                + "    { SELECT ('total' AS ?g) (?x AS ?c)\n"
                + "      { SELECT (sum(?c) AS ?x) {\n"
                + "        { SERVICE <cache:> { SELECT (count(*) AS ?c) { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g } }\n"
                + "      } } \n"
                + "    }\n"
                + "  # FILTER(CONTAINS(STR(?g), 'power')) # hier einfach aendern\n"
                + "}";

        int actualRowCount = TS_ResultSetLimits.testWithCleanCaches(dataset, queryStr, 1000);
        Assert.assertEquals(4, actualRowCount);
    }
}
