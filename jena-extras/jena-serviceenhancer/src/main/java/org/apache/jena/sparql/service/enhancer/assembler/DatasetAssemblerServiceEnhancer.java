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

import java.util.Objects;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.assembler.DatasetAssembler;
import org.apache.jena.sparql.service.enhancer.init.InitServiceEnhancer;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.graph.GraphUtils;


/**
 * Assembler that sets up a base dataset's context with the service enhancer machinery.
 * As changes are only applied to the context the resulting dataset is the base dataset itself.
 */
public class DatasetAssemblerServiceEnhancer
    extends DatasetAssembler
{
    @Override
    public DatasetGraph createDataset(Assembler a, Resource root) {

        Resource baseDatasetRes = GraphUtils.getResourceValue(root, VocabServiceEnhancer.baseDataset);
        Objects.requireNonNull(baseDatasetRes, "No ja:baseDataset specified on " + root);
        Object obj = a.open(baseDatasetRes);

        Dataset result;
        if (obj instanceof Dataset) {
            result = (Dataset)obj;
            Context cxt = result.getContext();
            InitServiceEnhancer.wrapOptimizer(cxt, ARQ.getContext());

            RDFNode selfIdRes = GraphUtils.getAsRDFNode(root, VocabServiceEnhancer.selfId);

            Node selfId = selfIdRes == null
                    ? baseDatasetRes.asNode()
                    : selfIdRes.asNode();

            cxt.set(InitServiceEnhancer.selfId, selfId);
            Log.info(DatasetAssemblerServiceEnhancer.class, "Dataset self id set to " + selfId);

        } else {
            Class<?> cls = obj == null ? null : obj.getClass();
            throw new AssemblerException(root, "Expected ja:baseDataset to be a Dataset but instead got " + Objects.toString(cls));
        }

        return result.asDatasetGraph();
    }
}
