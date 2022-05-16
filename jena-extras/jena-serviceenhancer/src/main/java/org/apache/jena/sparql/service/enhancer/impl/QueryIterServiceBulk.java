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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.atlas.iterator.IteratorOnClose;
import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.ext.com.google.common.collect.Iterators;
import org.apache.jena.ext.com.google.common.collect.Range;
import org.apache.jena.ext.com.google.common.collect.RangeMap;
import org.apache.jena.ext.com.google.common.collect.RangeSet;
import org.apache.jena.ext.com.google.common.collect.TreeBasedTable;
import org.apache.jena.ext.com.google.common.collect.TreeRangeMap;
import org.apache.jena.ext.com.google.common.collect.TreeRangeSet;
import org.apache.jena.ext.com.google.common.math.LongMath;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterConvert;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.engine.iterator.QueryIteratorMapped;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.service.enhancer.claimingcache.RefFuture;
import org.apache.jena.sparql.service.enhancer.impl.IteratorFactoryWithBuffer.SubIterator;
import org.apache.jena.sparql.service.enhancer.impl.util.BindingUtils;
import org.apache.jena.sparql.service.enhancer.impl.util.QueryIterDefer;
import org.apache.jena.sparql.service.enhancer.impl.util.QueryIterSlottedBase;
import org.apache.jena.sparql.service.enhancer.slice.api.IteratorOverReadableChannel;
import org.apache.jena.sparql.service.enhancer.slice.api.ReadableChannel;
import org.apache.jena.sparql.service.enhancer.slice.api.ReadableChannelOverSliceAccessor;
import org.apache.jena.sparql.service.enhancer.slice.api.ReadableChannelWithLimit;
import org.apache.jena.sparql.service.enhancer.slice.api.Slice;
import org.apache.jena.sparql.service.enhancer.slice.api.SliceAccessor;
import org.apache.jena.sparql.util.NodeFactoryExtra;

/**
 * QueryIter to process service requests in bulk with support for streaming caching.
 *
 * The methods closeIterator and moveToNext are synchronized.
 *
 */
public class QueryIterServiceBulk
    extends QueryIterSlottedBase<Binding>
{
    protected OpServiceInfo serviceInfo;
    protected ServiceCacheKeyFactory cacheKeyFactory;

    protected BatchQueryRewriter batchQueryRewriter;

    // protected boolean useLoopJoin;
    protected OpServiceExecutor opExecutor;
    protected ExecutionContext execCxt;

    protected List<Binding> inputs;

    protected ServiceResultSizeCache resultSizeCache;
    protected ServiceResponseCache cache;

    protected CacheMode cacheMode;

    // If true then data will only be written to the cache but not served from it
    // protected boolean cacheRefresh;

    // protected BatchRequestExecutor currentBatchExecution;

    protected Node targetService;

    protected int currentInputId = -1; // the binding currently being served from the batch

    protected int currentRangeId = -1;
    // protected long currentBindingCount; // the number of bindings processed for the current idx


    // The number of bindings served for the current inputId
    protected long currentInputIdBindingsServed;
    // protected boolean endMarkerSeen;

    // Cached attribute from BatchQueryRewriter.getIdxVar()
    protected Var idxVar;

    protected int maxBufferSize = 100000;
    protected int maxSkipCount = 10000;

    // Cache items in blocks of that many bindings; avoids synchronization on every binding
    protected int cacheBulkSize = 128;

    protected TreeBasedTable<Integer, Integer, Integer> inputToRangeToOutput = TreeBasedTable.create();

    // This is the reverse mapping of the table above; PartitionKey = (inputId, rangeId)
    protected Map<Integer, PartitionKey> outputToPartKey = new HashMap<>();

    // The set of outputIds that are served from the backend (absent means served from cache)
    // protected Set<Integer> backendOutputs;
    protected Set<PartitionKey> partKeysForBackend = new HashSet<>(); // The partitions served from the backend

    // The query iterator of the active bulk request
    protected SubIterator<Binding, QueryIterator> backendIt;

    protected Map<PartitionKey, QueryIterPeek> partKeyToIter = new HashMap<>();

    // Cached pointer of partKeyToIter.get(new PartitionKey()
    //protected QueryIterPeek activeIt = null; // QueryIterNullIterator.

    protected Estimate<Long> backendResultSetLimit;


    // Map for when an iterator can be closed
    // While outputToIter can have multiple ids mapping to the same iterator, this map
    // holds each iterator only once with its highest id.
    // protected Map<PartitionKey, QueryIterPeek> partKeyToClose = new HashMap<>();
    protected Set<PartitionKey> partKeyToClose = new HashSet<>();

    public QueryIterServiceBulk(
            OpServiceInfo serviceInfo,
            BatchQueryRewriter batchQueryRewriter,
            ServiceCacheKeyFactory cacheKeyFactory,
            // boolean useLoopJoin,
            OpServiceExecutor opExecutor,
            ExecutionContext execCxt,
            List<Binding> inputs,
            ServiceResultSizeCache resultSizeCache,
            ServiceResponseCache cache,
            CacheMode cacheMode
            // boolean refreshCache
        ) {
        this.serviceInfo = serviceInfo;
        this.cacheKeyFactory = cacheKeyFactory;
        // this.useLoopJoin = useLoopJoin;
        this.opExecutor = opExecutor;
        this.execCxt = execCxt;
        this.inputs = inputs;
        this.resultSizeCache = resultSizeCache;
        this.cacheMode = cacheMode;
        this.cache = cache;
        // this.cacheRefresh = cacheRefresh;
        this.batchQueryRewriter = batchQueryRewriter;

        this.idxVar = batchQueryRewriter.getIdxVar();

        // multiple injections of an idxVar which need to be kept separate
        // Set<Var> visibleServiceSubOpVars = serviceInfo.getVisibleSubOpVarsScoped();
        // this.idxVar = VarUtils.freshVar("__idx__", visibleServiceSubOpVars);
        // ExecutionContext execCxt = opExecutor.getExecCxt();

        // Initialize with empty iterator
        // this.activeIt = QueryIterPeek.create(QueryIterPlainWrapper.create(Collections.<Binding>emptyList().iterator(), execCxt), execCxt);


        this.targetService = serviceInfo.getServiceNode();
    }

    public Var getIdxVar() {
        return idxVar;
    }

    protected void advanceInput(boolean resetRangeId) {
        ++currentInputId;
        currentInputIdBindingsServed = 0;

        if (resetRangeId) {
            currentRangeId = 0;
        }
    }

    @Override
    protected synchronized Binding moveToNext() {

        // Binding parentBinding = null;
        // Binding childBindingWithIdx = null;
        Binding mergedBindingWithIdx = null;

        // One time init
        if (currentInputId < 0) {
            ++currentInputId;
            currentRangeId = 0;
            prepareNextBatchExec(false);
        }

        // Peek the next binding on the active iterator and verify that it maps to the current
        // partition key
        outer: while (true) {

            PartitionKey partKey = new PartitionKey(currentInputId, currentRangeId);
            QueryIterPeek activeIt = partKeyToIter.get(partKey);

            if (activeIt == null) {
                // Must advance to next scheduled iterator (may turn out that there is none)
                break;
            }

            boolean isBackendIt = partKeysForBackend.contains(partKey);

            if (isBackendIt && !activeIt.hasNext()) {
                Log.debug(QueryIterServiceBulk.class, "Iterator ended without end marker - assuming remote result set limit reached");
                long seenBackendData = backendIt.getOffset();
                backendResultSetLimit = new Estimate<Long>(seenBackendData, true);
                if (seenBackendData <= 0) {
                    Log.warn(QueryIterServiceBulk.class, "Known result set limit of " + seenBackendData + " detected");
                }

                resultSizeCache.updateLimit(targetService, backendResultSetLimit);

                // We obtained to few data for the current id - repeat the request
                prepareNextBatchExec(false);
                // currentRangeId = 0;
                continue;
//                if (currentInputIdBindingsServed < seenBackendData) {
//
//                }
            }

            // Refresh the result set limit in case there was a concurrent update
            if (backendResultSetLimit == null || currentInputIdBindingsServed >= backendResultSetLimit.getValue()) {
                backendResultSetLimit = resultSizeCache.getLimit(targetService);

                // delta: The number of rows the backend needs to deliver before we can serve any
                // aditional row to the client.
                @SuppressWarnings("unused") // For debugging
                long totalNeededBackendRowCount;
                long obtainedRowCount = 0;


                // The following loop decides whether another binding can be served from activeIt without
                // violating result size limits
                // The 'worst' outcome is that the request needs to be repeated because the result size
                // limit could not be determined within the thresholds

                // If the number of served bindings equals the backend result set limit we need at least one more binding
                while ((totalNeededBackendRowCount = (currentInputIdBindingsServed - backendResultSetLimit.getValue() + 1)) > 0) {

                    long remainingNeededBackendRowCount = 0;

                    // TODO We could rely on the backend iterator to update the cache with the known size

                    // If the limit is unknown we can try to buffer in the hope to make it know
                    //   if the limit is then still unknown we need to reset
                    if (backendIt != null && !backendResultSetLimit.isExact()) {
                        // Log.debug(QueryIterServiceBulk.class, String.format("Analyzing result set size limit whether %d bindings can be served. Current limit %d", currentInputIdBindingsServed, backendResultSetLimit.getValue()));

                        // Subtract the rows that have already been delivered by the backend
                        try (SubIterator<Binding, ?> subIt = backendIt.subIteratorAtEndOfBuffer()) {

                            long deliveredBackendRowCount = subIt.getOffset();
                            remainingNeededBackendRowCount = backendResultSetLimit.getValue() - deliveredBackendRowCount + 1;

                            // long remainingBuffer = maxBufferSize - (deliveredBackendRowCount - backendIt.getOffset());

                            // boolean resetRequest = false;

                            // If there is insufficient buffer available we can still try whether we see a result set limit
                            // alternatively we could just set resetRequest to true
                            // boolean isSufficientBufferAvailable = remainingNeededBackendRowCount < remainingBuffer;

                            // We can start returning bindings as soon as
                            // backendResultSetLimit > currentInputIdBindingsServed
                            // int obtainedRowCount = 0;
                            // boolean isEndMarkerSeen = false;
                            boolean isResultSetLimitReached = false; // reached end without seeing the end marker
                            while (obtainedRowCount < remainingNeededBackendRowCount) { // Repeat until we can serve another binding

                                if (subIt.hasNext()) {
                                    Binding binding = subIt.next();
                                    int inputId = getPartKeyFromBinding(binding).getInputId();
                                    boolean isEndMarkerSeen = BatchQueryRewriter.isRemoteEndMarker(inputId);
                                    if (isEndMarkerSeen) {
                                        // Ensure subIt's offset points past the end marker
                                        Iterators.size(subIt);
                                        break;
                                    } else {
                                        ++obtainedRowCount;
                                    }
                                } else {
                                    isResultSetLimitReached = true;
                                    break;
                                }
                            }

                            long seenBackendData = subIt.getOffset();
                            backendResultSetLimit = new Estimate<Long>(seenBackendData, isResultSetLimitReached);
                            resultSizeCache.updateLimit(targetService, backendResultSetLimit);
                        }

                        if (obtainedRowCount < remainingNeededBackendRowCount) {
                            // This creates a request that bypasses the cache on the first input
                            // (i.e. may retrieve data previously served from the cache)
                            // but also cuts away already retrieved (and returned) bindings.
                            prepareNextBatchExec(true);
                            continue outer;
                        }

                        // remainingNeededBackendRowCount -= obtainedRowCount;
                    }

                    // Check if we are going to serve too many results for the current binding
                    if (backendResultSetLimit.isExact() && currentInputIdBindingsServed >= backendResultSetLimit.getValue()) {
                        // Skip until we reach the next id
                        // If we need data from cache we can just increment currentInputId
                        // If we need to serve from the backend then try to skip
                        if (isBackendIt) {
                            int skipCount = 0;

                            while (backendIt.hasNext() && skipCount++ < maxSkipCount) {
                                Binding peek = backendIt.peek();
                                int peekInputId = getPartKeyFromBinding(peek).getInputId();

                                if (peekInputId != currentInputId) {
                                    advanceInput(true);
                                    continue outer;
                                } else {
                                    backendIt.next();
                                }
                            }

                            // Cut off the iterator so we move to the next input
                            activeIt = null; // QueryIterPeek.create(new QueryIterNullIterator(execCxt), execCxt);
                            break;

                            // If we could not skip past the inputId within the read ahead
                            // then give up and execute a fresh query
                            // We also come here if the backend iterator was exhausted
                            // ++currentInputId;
                            // currentInputIdBindingsServed = 0;
                            // advanceInput(true);
                            // prepareNextBatchExec(false);
                            // continue outer;
                        } else {
                            // Skip over the cache entry and skip to the next input
                            if (activeIt != null) {
                                activeIt.close();
                            }
                            advanceInput(true);
                            continue outer;
                        }
                    }

                    if (backendIt == null) {
                        prepareNextBatchExec(true);
                        activeIt = partKeyToIter.get(partKey);
                        // continue outer;
                    }

                    // Note: we only need to skip over excessive data once we need to fetch data from the backend iterator
                    // Conversely, we skip over iterators backed by the cache

                    // If the limit is known then we need to skip over excessive backend data
                    //   if there is too much data to skip at some point we give up and reset the request


                    // We need to skip ahead on iterator over the backend (not over the cache)
                    // skipOrReset(activeIt);
                }
            }

            // PartitionKey partKey = new PartitionKey(currentInputId, currentRangeId);
            if (activeIt != null) {
                if (activeIt.hasNext()) {
                    Binding peek = activeIt.peek();
                    int peekOutputId = BindingUtils.getNumber(peek, idxVar).intValue();
                    if (BatchQueryRewriter.isRemoteEndMarker(peekOutputId)) {
                        // Attempt to move to the next range
                        ++currentRangeId;
                        continue;

                        // System.out.println("Fetch exhausted");
                        // advanceInput(true);


                        // FIXME We may have obtained to few results
                        // prepareNextBatchExec(false);
                        // continue;
                        //continue;
                        // break;
                    }

                    PartitionKey pk = outputToPartKey.get(peekOutputId);

                    if (pk == null) {
                        throw new IllegalStateException(
                                String.format("An output binding referred to an input id without corresponding input binding. Referenced input id %1$d, Output binding: %2$s", peekOutputId, peek));
                    }

                    boolean matchesCurrentPartition = pk.getInputId() == currentInputId &&
                            pk.getRangeId() == currentRangeId;

                    if (matchesCurrentPartition) {
                        Binding parentBinding = inputs.get(currentInputId);
                        Binding childBindingWithIdx = activeIt.next();

                        // Check for compatibility
                        mergedBindingWithIdx = Algebra.merge(parentBinding, childBindingWithIdx);
                        if (mergedBindingWithIdx == null) {
                            continue;
                        } else {
                            break;
                        }
                    }
                } else {
                    // If we come here it means:
                    // - no end marker was present
                    // - no more data available
                    // If our request ended prematurely fetch more data
                    //prepareNextBatchExec(false);
                    // continue;
                }
            }

            // Cleanup of no longer needed resources
            PartitionKey pk = new PartitionKey(currentInputId, currentRangeId);
            // Integer outputId = inputToRangeToOutput.get(currentInputId, currentRangeId);

            if (partKeyToClose.contains(pk)) {
                // System.out.println("Closing part key " + pk);
                Closeable closeable = partKeyToIter.get(pk);
                closeable.close();
                partKeyToClose.remove(pk);
            }
            inputToRangeToOutput.remove(currentInputId, currentRangeId);
            partKeyToIter.remove(pk);

            // Increment rangeId/inputId until we reach the end
            ++currentRangeId;
            SortedMap<Integer, Integer> row = inputToRangeToOutput.row(currentInputId);
            if (!row.containsKey(currentRangeId)) {
                advanceInput(true);
            }

            // Check if we need to load the next batch
            // TODO If there are missing (=non-loaded) rows within the read ahead range then load them
//            if (!inputToRangeToOutput.containsRow(currentInputId)) {
//                if (batchIterator.hasNext()) {
//                    prepareNextBatchExec();
//                }
//            }

            // If there is still no further batch then we assume we reached the end
            if (!inputToRangeToOutput.containsRow(currentInputId)) {
                break;
            }

            // outputId = inputToRangeToOutput.get(currentInputId, currentRangeId);
            // activeIt = partKeyToIter.get(pk);
        }


        // Remove the idxVar from the childBinding
        Binding result = null;
        if (mergedBindingWithIdx != null) {
            ++currentInputIdBindingsServed;

            int outputId = BindingUtils.getNumber(mergedBindingWithIdx, idxVar).intValue();
            PartitionKey pk = outputToPartKey.get(outputId);
            int inputId = pk.getInputId();
            Binding tmp = BindingUtils.project(mergedBindingWithIdx, mergedBindingWithIdx.vars(),
                    Collections.singleton(idxVar));



            // Binding childBinding = BindingUtils.project(childBindingWithIdx, childBindingWithIdx.vars(), Collections.singleton(idxVar));
            result = BindingFactory.binding(tmp, idxVar, NodeValue.makeInteger(inputId).asNode());

            // childBindingWithIdx;

            // result = BindingFactory.builder(parentBinding).addAll(childBinding).build();
        }

        if (result == null) {
            freeResources();
        }

        // System.err.println("Serving #" + currentInputIdBindingsServed + ": " + result);
        return result;
    }

    public PartitionKey getPartKeyFromBinding(Binding binding) {
        int peekOutputId = BindingUtils.getNumber(binding, idxVar).intValue();

        PartitionKey result = BatchQueryRewriter.isRemoteEndMarker(peekOutputId)
                ? new PartitionKey(BatchQueryRewriter.REMOTE_END_MARKER, 0)
                : outputToPartKey.get(peekOutputId);

        return result;
    }


    protected void freeResources() {
        if (backendIt != null) {
            backendIt.close();
        }

        for (PartitionKey partKey : partKeyToClose) {
            Closeable closeable = partKeyToIter.get(partKey);
            closeable.close();
        }
        partKeyToClose.clear();

        inputToRangeToOutput.clear();
        outputToPartKey.clear();
        // partKeyToIter.values().forEach(QueryIterator::close);
        partKeyToIter.clear();

        partKeysForBackend.clear();
    }

    @Override
    public synchronized void closeIterator() {
        freeResources();
    }

    /** Prepare the lazy execution of the next batch and register all iterators with {@link #nextOutputIdToIterator} */
    // seqId = sequential number injected into the request
    // inputId = id (index) of the input binding
    // rangeId = id of the range w.r.t. to the input binding
    // partitionKey = (inputId, rangeId)
    public void prepareNextBatchExec(boolean bypassCacheOnFirstInput) {

        freeResources();

        // NavigableMap<Integer, Binding> batchItems = null;
        // Set<Var> bindingVarsMentionedScoped = BindingUtils.varsMentioned(inputs);

        // Set<Var> joinVars;

        // Binding to var of query
//        Map<Var, Var> joinVarMap;
//        if (useLoopJoin) {
//            Set<Var> rhsPlainVars = serviceInfo.getMentionedVarsPlain();
//            joinVarMap = VarUtils.reverseVarRenameMap(bindingVarsMentionedScoped).entrySet().stream()
//                .filter(e -> rhsPlainVars.contains(e.getValue()))
//                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
//
//            // reverseVarRenameMap;
//            // Set<Var> bindingVarsMentionedPlain = VarUtils.reverseVarRename(bindingVarsMentionedScoped);
//            // joinVars = Sets.intersection(bindingVarsMentionedPlain, serviceInfo.getMentionedSubOpVarsPlain());
//        } else {
////            Set<Var> rhsVisibleVars = serviceInfo.getVisibleSubOpVarsScoped();
////            joinVarMap = VarUtils.reverseVarRenameMap(bindingVarsMentionedScoped).entrySet().stream()
////                    .filter(e -> rhsVisibleVars.contains(e.getValue())
////                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
////
//
//            Set<Var> joinVars = Sets.intersection(bindingVarsMentionedScoped, serviceInfo.getVisibleSubOpVarsScoped());
//            joinVarMap = VarUtils.reverseVarRenameMap(joinVars);
//
//
//        }

        // BatchQueryRewriter rewriter = new BatchQueryRewriter(serviceInfo, idxVar);


        // List<PartitionRequest<Binding>> backendRequests = new ArrayList<>();
        Batch<Integer, PartitionRequest<Binding>> backendRequests = BatchImpl.forInteger();

        // inputToBinding.putAll(batchItems);


        Estimate<Long> serviceDescription = resultSizeCache.getLimit(targetService);
        long resultSetLimit = serviceDescription.getValue();
        boolean isExact = serviceDescription.isExact(); // we interpret the limit as a lower bound if exact is false!

        // TODO If the result set limit is known then restrict the iterators to it

        int nextAllocOutputId = 0;
        int batchSize = inputs.size();

        Log.info(QueryIterServiceBulk.class, "Schedule for current batch:");
        int rangeId = currentRangeId;

        for (int inputId = currentInputId; inputId < batchSize; ++inputId) {

            boolean isFirstInput = inputId == currentInputId;

            long displacement = isFirstInput && !bypassCacheOnFirstInput
                    ? currentInputIdBindingsServed
                    : 0l
                    ;

            Binding inputBinding = inputs.get(inputId);
            // Binding joinBinding = new BindingProject(joinVarMap.keySet(), inputBinding);

            Slice<Binding[]> slice = null;
            Lock lock = null;
            RefFuture<ServiceCacheValue> cacheValueRef = null;

            if (cache != null) {

                ServiceCacheKey cacheKey = cacheKeyFactory.createCacheKey(inputBinding);
                // ServiceCacheKey cacheKey = new ServiceCacheKey(targetService, serviceInfo.getRawQueryOp(), joinBinding, useLoopJoin);
                // System.out.println("Lookup with cache key " + cacheKey);

                // FIXME cacheValueRef must be closed as part of the iterators that read from the cache
                cacheValueRef = cache.getCache().claim(cacheKey);
                ServiceCacheValue serviceCacheValue = cacheValueRef.await();

                // Lock an existing cache entry so we can read out the loaded ranges
                slice = serviceCacheValue.getSlice();

                if (CacheMode.CLEAR.equals(cacheMode)) {
                    slice.clear();
                }

                lock = slice.getReadWriteLock().readLock();

                Log.debug(QueryIterServiceBulk.class, "Created cache key: " + cacheKey);
                // Log.debug(BatchRequestIterator.class, "Cached ranges: " + slice.getLoadedRanges().toString());

                lock.lock();
            }

            RangeSet<Long> loadedRanges;
            long knownSize;
            try {
                if (slice != null) {
                    loadedRanges = slice.getLoadedRanges();
                    knownSize = slice.getKnownSize();
                } else {
                    loadedRanges = TreeRangeSet.create();
                    knownSize = -1;
                }

                // Iterate the present/absent ranges
                long start = serviceInfo.getOffset();
                if (start == Query.NOLIMIT) {
                    start = 0;
                }

                long baseLimit = serviceInfo.getLimit();
                if (baseLimit < 0) {
                    baseLimit = Long.MAX_VALUE;
                }

                long limit = baseLimit;
                if (isExact && baseLimit >= 0) {
                    limit = Math.min(limit, resultSetLimit);
                }

                if (displacement != 0) {
                    start += displacement;
                    if (limit != Long.MAX_VALUE) {
                        limit -= displacement;
                    }
                }

                long max = knownSize < 0 ? Long.MAX_VALUE : knownSize;
                long end = limit == Long.MAX_VALUE ? max : LongMath.saturatedAdd(start, limit);
                end = Math.min(end, max);

                Range<Long> requestedRange = end == Long.MAX_VALUE
                    ? Range.atLeast(start)
                    : Range.closedOpen(start, end);

                RangeMap<Long, Boolean> allRanges = TreeRangeMap.create();
                if (bypassCacheOnFirstInput && isFirstInput) {
                    allRanges.put(requestedRange, false);
                    // FIXME if we bypass the cache we need to skip the bindings already served
                    //    'currentInputIdBindingsServed'
                } else {
                    RangeSet<Long> presentRanges = loadedRanges.subRangeSet(requestedRange);
                    RangeSet<Long> absentRanges = loadedRanges.complement().subRangeSet(requestedRange);

                    presentRanges.asRanges().forEach(r -> allRanges.put(r, true));
                    absentRanges.asRanges().forEach(r -> allRanges.put(r, false));
                }

                // If the beginning of the request range is covered by a cache then serve from it
                // a global limit may prevent having to fire a backend request
                // However, as soon as we have to fire a backend request we need to ensure we don't serve
                // more data then the seen result set limit
                // If the size of the data can be greater than that limit then:
                //   - We need to start the backend request from the request offset
                //   - The issue is how to handle the next binding

                Log.info(QueryIterServiceBulk.class, "input " + inputId + ": " +
                    allRanges.toString()
                        .replace("false", "fetch")
                        .replace("true", "cached"));

                Map<Range<Long>, Boolean> mapOfRanges = allRanges.asMapOfRanges();

                if (mapOfRanges.isEmpty()) {
                    // Special case if it is known that there are no bindings:
                    // Register an empty iterator
                    PartitionKey partitionKey = new PartitionKey(inputId, rangeId);
                    QueryIterPeek it = QueryIterPeek.create(new QueryIterNullIterator(execCxt), execCxt);
                    partKeyToIter.put(partitionKey, it);
                    partKeyToClose.add(partitionKey); // it);

                    // Close the cache ref immediately
                    if (cacheValueRef != null) {
                        cacheValueRef.close();
                    }
                } else {
                    Iterator<Entry<Range<Long>, Boolean>> rangeIt = mapOfRanges.entrySet().iterator();

                    RefFuture<ServiceCacheValue> finalCacheValueRef = cacheValueRef;

                    boolean usesCacheRead = false;
                    while (rangeIt.hasNext()) {
                        PartitionKey partitionKey = new PartitionKey(inputId, rangeId);
                        Entry<Range<Long>, Boolean> f = rangeIt.next();

                        Range<Long> range = f.getKey();
                        boolean isLoaded = f.getValue();

                        long lo = range.lowerEndpoint();
                        long hi = range.hasUpperBound() ? range.upperEndpoint() : Long.MAX_VALUE;
                        long lim = hi == Long.MAX_VALUE ? Long.MAX_VALUE : hi - lo;

                        if (isLoaded) {
                            usesCacheRead = true;
                            SliceAccessor<Binding[]> accessor = slice.newSliceAccessor();

                            // Prevent eviction of the scheduled range
                            accessor.addEvictionGuard(Range.closedOpen(lo, hi));

                            // Create a channel over the accessor for sequential reading
                            // Reading from the channel internally advances the range of data claimed by the accessor
                            // Note: As an improvement the eviction guard could be managed by the channel so that data is immediately released after read.
                            ReadableChannel<Binding[]> channel =
                                    new ReadableChannelWithLimit<>(
                                            new ReadableChannelOverSliceAccessor<>(accessor, lo),
                                            lim);

                            // CloseableIterator<Binding> baseIt = ReadableChannels.newIterator(channel);
                            IteratorCloseable<Binding> baseIt = new IteratorOverReadableChannel<>(channel.getArrayOps(), channel, 1024 * 4);

                            // The last iterator's close method also unclaims the cache entry
                            Runnable cacheEntryCloseAction = rangeIt.hasNext() || finalCacheValueRef == null
                                    ? baseIt::close
                                    : () -> {
                                        baseIt.close();
                                        finalCacheValueRef.close();
                                    };

                            // Bridge the cache iterator to jena
                            QueryIterator qIterA = QueryIterPlainWrapper.create(Iter.onClose(baseIt, cacheEntryCloseAction), execCxt);

                            Map<Var, Var> normedToScoped = serviceInfo.getVisibleSubOpVarsNormedToScoped();
                            qIterA = new QueryIteratorMapped(qIterA, normedToScoped);

                            // Add a pseudo idxVar mapping
                            final long idxVarValue = nextAllocOutputId;
                            QueryIterConvert qIterB = new QueryIterConvert(qIterA, b ->
                                BindingFactory.binding(b, idxVar, NodeFactoryExtra.intToNode(idxVarValue)), execCxt);

                            QueryIterPeek it = QueryIterPeek.create(qIterB, execCxt);

                            partKeyToIter.put(partitionKey, it);
                            partKeyToClose.add(partitionKey); // it);
                        } else {
                            PartitionRequest<Binding> request = new PartitionRequest<>(nextAllocOutputId, inputBinding, lo, lim);
                            backendRequests.put(nextAllocOutputId, request);
                            partKeysForBackend.add(partitionKey);
                        }

                        inputToRangeToOutput.put(inputId, rangeId, nextAllocOutputId);
                        outputToPartKey.put(nextAllocOutputId, partitionKey);

                        ++rangeId;
                        ++nextAllocOutputId;
                    }

                    // Close the reference to the cache entry; QueryIterCaching will manage
                    // claim/unclaim in batches so we don't need to leave the reference open here
                    if (!usesCacheRead && finalCacheValueRef != null) {
                        finalCacheValueRef.close();
                    }
                }
            } finally {
                if (lock != null) {
                    lock.unlock();
                }
            }

            rangeId = 0;
        }


        // Create *deferred* a remote execution if needed
        // A limit on the query may cause the deferred execution to never run
        if (!backendRequests.isEmpty()) {
            BatchQueryRewriteResult rewrite = batchQueryRewriter.rewrite(backendRequests);
            // System.out.println(rewrite);

            Op newSubOp = rewrite.getOp();
            OpService substitutedOp = new OpService(targetService, newSubOp, serviceInfo.getOpService().getSilent());

            // Execute the batch request and wrap it such that ...
            // (1) we can merge it with other backend and cache requests in the right order
            // (2) responses are written to the cache
            Supplier<QueryIterator> qIterSupplier = () -> {
                QueryIterator r = opExecutor.exec(substitutedOp);
                return r;
            };

            QueryIterator qIter = new QueryIterDefer(qIterSupplier);

            // Wrap the iterator such that the items are cached
            if (cache != null) {
                //  serviceInfo.getRawQueryOp()
                qIter = new QueryIterWrapperCache(qIter, cacheBulkSize, cache, cacheKeyFactory, backendRequests, idxVar, targetService);
            }

            // Apply renaming after cache to avoid mismatch between op and bindings
            qIter = QueryIter.map(qIter, rewrite.getRenames());

            // System.out.println("renames " + rewrite.getRenames());

            // Wrap the iterator further to detect resultset limit condition

            // Wrap the query iter such that we can peek the next binding in order
            // to decide from which iterator to take the next element
            SubIterator<Binding, QueryIterator> backendItPrimary = IteratorFactoryWithBuffer.wrap(qIter);
            IteratorOnClose<Binding> jenaIt = Iter.onClose(backendItPrimary, qIter::close);
            QueryIterator iter = QueryIterPlainWrapper.create(jenaIt, execCxt);

            QueryIterPeek frontIter = QueryIterPeek.create(iter, execCxt);

            // Register the iterator for the backend request with all corresponding outputIds
            for (Integer outputId : backendRequests.getItems().keySet()) {
                PartitionKey partKey = outputToPartKey.get(outputId);
                partKeyToIter.put(partKey, frontIter);
            }

            int lastOutputId = backendRequests.getItems().lastKey();
            PartitionKey lastPartKey = outputToPartKey.get(lastOutputId);
            partKeyToClose.add(lastPartKey); // frontIter);

            backendIt = backendItPrimary;

            if (bypassCacheOnFirstInput) {
                // If we come here then a number fo bindings was handed to the client
                // but then we weren't sure whether we can deliver any more w.r.t. the
                // backend result set size limit - consume as many bindings already handed to the client
                for (int i = 0; i < currentInputIdBindingsServed; ++i) {
                    if (backendIt.hasNext()) {
                        backendIt.next();
                    }
                }
            }
        }


    }

    protected int getOutputId(Binding binding) {
        int result = BindingUtils.getNumber(binding, idxVar).intValue();
        return result;
    }

    protected PartitionKey getPartitionKeyForOutputId(int outputId) {
        return outputToPartKey.get(outputId);
    }

}

