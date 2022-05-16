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

package org.apache.jena.sparql.service.enhancer.init;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.assemblers.AssemblerGroup;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.SystemARQ;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.algebra.optimize.Optimize;
import org.apache.jena.sparql.algebra.optimize.Rewrite;
import org.apache.jena.sparql.algebra.optimize.RewriteFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.service.ServiceExecutorRegistry;
import org.apache.jena.sparql.service.enhancer.algebra.TransformSE_EffectiveOptions;
import org.apache.jena.sparql.service.enhancer.algebra.TransformSE_JoinStrategy;
import org.apache.jena.sparql.service.enhancer.assembler.DatasetAssemblerServiceEnhancer;
import org.apache.jena.sparql.service.enhancer.assembler.VocabServiceEnhancer;
import org.apache.jena.sparql.service.enhancer.impl.ChainingServiceExecutorBulkServiceEnhancer;
import org.apache.jena.sparql.service.enhancer.impl.ServiceOpts;
import org.apache.jena.sparql.service.enhancer.impl.ServiceResponseCache;
import org.apache.jena.sparql.service.enhancer.impl.ServiceResultSizeCache;
import org.apache.jena.sparql.service.single.ChainingServiceExecutor;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.Symbol;
import org.apache.jena.sys.JenaSubsystemLifecycle;

public class InitServiceEnhancer
    implements JenaSubsystemLifecycle
{
    /** An IRI constant for referencing the active dataset within a SERVICE clause */
    public static final Node SELF = NodeFactory.createURI("urn:x-arq:self");

    /** Maximum number of bindings to group into a single bulk request; restricts serviceBulkRequestItemCount */
    public static final Symbol serviceBulkMaxBindingCount = SystemARQ.allocSymbol("serviceBulkMaxBindingCount") ;

    /** Number of bindings to group into a single bulk request */
    public static final Symbol serviceBulkBindingCount = SystemARQ.allocSymbol("serviceBulkMaxBindingCount") ;

    public static final Symbol serviceCache = SystemARQ.allocSymbol("serviceCache") ;

    /** Service cache control password */
    public static final Symbol serviceCacheCtrlPwd = SystemARQ.allocSymbol("serviceCacheCtrlPwd") ;

    public static final Symbol serviceResultSizeCache = SystemARQ.allocSymbol("serviceResultSizeCache") ;


    public static final Symbol selfId = SystemARQ.allocSymbol("datasetSelfId") ;

    /**
     * A guide number to limit bulk SERVICE requests to roughly this byte size.
     * Implementations may use a heuristic to estimate the number of bytes in order to avoid
     * excessive string serializations of query/algebra objects.
     * For example, an approach may just sum up Binding.toString().
     * The limit is ignored for the first binding added to such a request
     */
    public static final Symbol serviceBulkRequestMaxByteSize = SystemARQ.allocSymbol("serviceBulkRequestMaxByteSize") ;


    public void start() {
        init();
    }

    @Override
    public void stop() {
    }


    public static void registerAssembler() {
        // Assembler.general.

        // Assembler ass = Assembler.general.assemblerFor(ResourceFactory.createResource(JA.getURI() + "ServicePlugin"));

    }

    public static void init() {
        ServiceResponseCache cache = new ServiceResponseCache();
        ARQ.getContext().put(serviceCache, cache);

        ServiceResultSizeCache resultSizeCache = new ServiceResultSizeCache();
        ServiceResultSizeCache.set(ARQ.getContext(), resultSizeCache);

        ServiceExecutorRegistry.get().addBulkLink(new ChainingServiceExecutorBulkServiceEnhancer());

        // Register SELF extension
        registerServiceExecutorSelf(ServiceExecutorRegistry.get());

        registerWith(Assembler.general);

//        registerFunctions(FunctionRegistry.get());
//        registerPFunctions(PropertyFunctionRegistry.get());
    }

//    public static void registerFunctions(FunctionRegistry reg) {
//        reg.put(ARQConstants.ARQFunctionLibraryURI + "cacheInvalidate", CacheInvalidate.class);
//    }
//
//    public static void registerPFunctions(PropertyFunctionRegistry reg) {
//        // reg.put(ARQConstants.ARQFunctionLibraryURI + "cacheInvalidate", CacheInvalidate.class);
//        //reg.put(ARQConstants.ARQPropertyFunctionLibraryURI + "cacheList", PropFuncCacheList.class);
//        reg.put("http://foo.bar/baz/" + "cacheList", cacheList.class);
//    }

    public static void registerServiceExecutorSelf(ServiceExecutorRegistry registry) {
        ChainingServiceExecutor selfExec = (opExec, opOrig, binding, execCxt, chain) -> {
            QueryIterator r;
            ServiceOpts so = ServiceOpts.getEffectiveService(opExec);
            OpService target = so.getTargetService();

            // It seems that we always need to run the optimizer here
            // in order to have property functions recognized properly
            if (SELF.equals(target.getService())) {
                String optimizerMode = so.getFirstValue(ServiceOpts.SO_OPTIMIZE, "on", "on");
                Op op = opExec.getSubOp();
                // Run the optimizer unless disabled
                if (!"off".equals(optimizerMode)) {
                    Context cxt = execCxt.getContext();
                    RewriteFactory rf = decideOptimizer(cxt);
                    Rewrite rw = rf.create(cxt);
                    op = rw.rewrite(op);
                }
                r = QC.execute(op, binding, execCxt);
            } else {
                r = chain.createExecution(opExec, opOrig, binding, execCxt);;
            }
            return r;
        };
        registry.addSingleLink(selfExec);
    }


    static void registerWith(AssemblerGroup g)
    {
        AssemblerUtils.registerDataset(VocabServiceEnhancer.DatasetServiceEnhancer, new DatasetAssemblerServiceEnhancer());

        // Graphs don't have a context so we can't install this plugin directly on them
        // Wrap graphs as datasets first.
    }


    /** If there is an optimizer in tgt that wrap it. Otherwise put a fresh optimizer into tgt
     * that lazily wraps the optimizer from src */
    public static void wrapOptimizer(Context tgt, Context src) {
        if (tgt == src) {
            throw new IllegalArgumentException("Target and source contexts for optimizer must differ to avoid infinite loop during lookup");
        }

        RewriteFactory baseFactory = tgt.get(ARQConstants.sysOptimizerFactory);
        if (baseFactory == null) {
            // Wrap the already present optimizer
            wrapOptimizer(tgt);
        } else {
            // Lazily delegate to the optimizer in src
            RewriteFactory factory = cxt -> op -> {
                RewriteFactory f = decideOptimizer(src);
                f = enhance(f);
                Context mergedCxt = Context.mergeCopy(src, cxt);
                Rewrite r = f.create(mergedCxt);
                return r.rewrite(op);
            };
            tgt.set(ARQConstants.sysOptimizerFactory, factory);
        }
    }

    public static RewriteFactory decideOptimizer(Context context) {
        RewriteFactory result = context.get(ARQConstants.sysOptimizerFactory);
        if (result == null) {
            result = Optimize.getFactory();

            if (result == null) {
                result = Optimize.stdOptimizationFactory;
            }
        }
        return result;
    }

    /** Register the algebra transformer that enables forcing linear joins via SERVICE <loop:>*/
    public static void wrapOptimizer(Context cxt) {
        RewriteFactory baseFactory = decideOptimizer(cxt);
        RewriteFactory enhancedFactory = enhance(baseFactory);
        cxt.set(ARQConstants.sysOptimizerFactory, enhancedFactory);
    }

    public static RewriteFactory enhance(RewriteFactory baseFactory) {
        RewriteFactory enhancedFactory = cxt -> {
            Rewrite baseRewrite = baseFactory.create(cxt);
            Rewrite[] rw = { null };
            rw[0] = op -> {
                Op a = Transformer.transform(new TransformSE_EffectiveOptions(), op);
                Op b = Transformer.transform(new TransformSE_JoinStrategy(), a);

                // Op c = Transformer.transform(new TransformSE_OptimizeSelfJoin(rw[0]), b);

                Op r = baseRewrite.rewrite(b);

                Op q = Transformer.transform(new TransformSE_JoinStrategy(), r);

//                Query f = OpAsQuery.asQuery(q);
//                System.out.println(f);

                return q;
            };
            return rw[0];
        };
        return enhancedFactory;
    }

    public static Node resolveServiceNode(Node node, ExecutionContext execCxt) {
        Node result = SELF.equals(node)
                ? resolveSelfId(execCxt)
                : node;

        return result;
    }

    public static Node resolveSelfId(ExecutionContext execCxt) {
        Context context = execCxt.getContext();

        Node id = context.get(selfId);
        if (id == null) {
            DatasetGraph dg = execCxt.getDataset();
            int hashCode = System.identityHashCode(dg);
            id = NodeFactory.createLiteral(SELF.getURI() + "@dataset" + hashCode);
        }

        return id;
    }
}
