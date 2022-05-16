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

import org.apache.jena.assembler.JA;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class VocabServiceEnhancer {
    private static final String NS = JA.getURI();

    public static String getURI() { return NS; }

    public static final Resource DatasetServiceEnhancer = ResourceFactory.createResource(NS + "DatasetServiceEnhancer");
    public static final Property baseDataset            = ResourceFactory.createProperty(NS + "baseDataset");

    /** The id (a node) to which to resolve urn:x-arq:self */
    public static final Property selfId                 = ResourceFactory.createProperty(NS + "selfId");

    public static final Resource ModelServiceEnhancer   = ResourceFactory.createResource(NS + "ModelServiceEnhancer");

    // 'baseModel' already defined in JA
}
