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

package org.apache.jena.sparql.service.enhancer.slice.api;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.jena.ext.com.google.common.base.Preconditions;
import org.apache.jena.ext.com.google.common.collect.ContiguousSet;
import org.apache.jena.ext.com.google.common.collect.DiscreteDomain;
import org.apache.jena.ext.com.google.common.collect.Range;
import org.apache.jena.ext.com.google.common.collect.RangeMap;
import org.apache.jena.ext.com.google.common.collect.RangeSet;
import org.apache.jena.sparql.service.enhancer.impl.util.RangeUtils;


/**
 * Metadata for slices of data.
 *
 * Holds information about
 * <ul>
 *   <li>the min/max number of known items</li>
 *   <li>loaded data ranges</li>
 *   <li>failed data ranges</li>
 * </ul>
 *
 * @author raven
 *
 */
public interface SliceMetaData
    extends Cloneable
{
    RangeSet<Long> getLoadedRanges();
    RangeMap<Long, List<Throwable>> getFailedRanges();

    long getMinimumKnownSize();
    long getMaximumKnownSize();

    SliceMetaData setMinimumKnownSize(long size);
    SliceMetaData setMaximumKnownSize(long size);

    /** A lock to control concurrent access to this object */
    ReadWriteLock getReadWriteLock();
    Condition getHasDataCondition();

    int getPageSize();

    /** Updates the maximum known size iff the argument is less than the current known maximum */
    default SliceMetaData updateMaximumKnownSize(long size) {
        long current = getMaximumKnownSize();

        if (size < current) {
            setMaximumKnownSize(size);
        }

        return this;
    }

    /** Updates the minimum known size iff the argument is graeter than the current known minimum */
    default SliceMetaData updateMinimumKnownSize(long size) {
        long current = getMinimumKnownSize();

        if (size > current) {
            setMinimumKnownSize(size);
        }

        return this;
    }

    default SliceMetaData setKnownSize(long size) {
        Preconditions.checkArgument(size >= 0, "Negative known size");

        setMinimumKnownSize(size);
        setMaximumKnownSize(size);

        return this;
    }

    /** -1 If not exactly known */
    default long getKnownSize() {
        boolean isExact = isExactSizeKnown();
        long result = isExact ? getMaximumKnownSize() : -1;
        return result;
    }

    default RangeSet<Long> getGaps(Range<Long> requestRange) {
        long maxKnownSize = getMaximumKnownSize();
        Range<Long> maxKnownRange = Range.closedOpen(0l, maxKnownSize);

        Range<Long> effectiveRequestRange = requestRange.intersection(maxKnownRange);

        RangeSet<Long> loadedRanges = getLoadedRanges();
        RangeSet<Long> result = RangeUtils.gaps(effectiveRequestRange, loadedRanges);
        return result;
    }

    default boolean isExactSizeKnown() {
        long minSize = getMinimumKnownSize();
        long maxSize = getMaximumKnownSize();

        boolean result = minSize == maxSize;
        return result;
    }

    /**
     * Whether all data has been loaded. This is the case if
     * the exact size is known and there is only a single range covering
     * [0, maxSize)
     *
     * @return
     */
    default boolean isComplete() {
        boolean result = false;

        boolean isExactSizeKnown = isExactSizeKnown();

        if (isExactSizeKnown) {
            long exactSize = getMaximumKnownSize();

            RangeSet<Long> ranges = getLoadedRanges();
            Set<Range<Long>> set = ranges.asRanges();

            if (set.size() == 1) {
                Range<Long> range = set.iterator().next();
                ContiguousSet<Long> cs = ContiguousSet.create(range, DiscreteDomain.longs());
                Long start = cs.first();
                Long end = cs.last();

                result = start != null && end != null && start == 0l && (end + 1) == exactSize;
            }
        }

        return result;
    }
}
