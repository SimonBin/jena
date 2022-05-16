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

package org.apache.jena.sparql.service.enhancer.algebra;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.service.enhancer.impl.ServiceOpts;

/**
 * Detects options on SERVICE and materializes them.
 * In the case of self-joins checks an optimizer will be run preemptively unless
 * the option 'optimizer:off' is present.
 *
 * SERVICE <loop:> {
 *   SERIVCE <bulk:> {
 *      SERVICE <https://dbpedia.org/sparql> { }
 *   }
 * }
 * becomes
 * SERVICE <loop:bulk:https://dbpedia.org/sparql> { }
 *
 */
public class TransformSE_EffectiveOptions
    extends TransformCopy
{
    @Override
    public Op transform(OpService opService, Op subOp) {
        OpService tmp = new OpService(opService.getService(), subOp, opService.getSilent());
        ServiceOpts so = ServiceOpts.getEffectiveService(tmp);
        OpService result = so.toService();
        return result;
    }
}
