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

package org.apache.jena.sparql.service.enhancer.assembler;

import java.io.StringReader;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Test;

public class TestDatasetAssemblerServiceEnhancer
{
    /**
     * This test case attempts to assemble a dataset with the service enhancer plugin
     * set up in its context. A query making use of enhancer features is fired against it.
     * Only if the plugin is loaded successfully then the query will execute successfully.
     */
    @Test
    public void testAssembler() {
        String specStr = String.join("\n",
            "@prefix ja: <http://jena.hpl.hp.com/2005/11/Assembler#> .",
            "<urn:root> a ja:DatasetServiceEnhancer ; ja:baseDataset <urn:base> .",
            "<urn:base> a ja:MemoryDataset ."
        );

        Model spec = ModelFactory.createDefaultModel();
        RDFDataMgr.read(spec, new StringReader(specStr), null, Lang.TURTLE);

        Dataset dataset = DatasetFactory.assemble(spec.getResource("urn:root"));

        try (QueryExecution qe = QueryExecutionFactory.create(
                "SELECT * { BIND(<urn:x> AS ?x) SERVICE <loop:bulk+10:> { ?x ?y ?z } }", dataset)) {
            ResultSetFormatter.consume(qe.execSelect());
        }
    }
}
