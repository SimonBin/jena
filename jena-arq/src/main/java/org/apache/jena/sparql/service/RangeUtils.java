package org.apache.jena.sparql.service;

import org.apache.jena.ext.com.google.common.collect.Range;
import org.apache.jena.ext.com.google.common.collect.RangeSet;
import org.apache.jena.query.Query;

public class RangeUtils {
    public static <C extends Comparable<C>> RangeSet<C> gaps(Range<C> request, RangeSet<C> ranges) {
        RangeSet<C> gaps = ranges.complement().subRangeSet(request);
        return gaps;
    }


    public static Range<Long> toRange(Query query) {
        Range<Long> result = toRange(query.getOffset(), query.getLimit());
        return result;
    }

    public static Range<Long> toRange(Long offset, Long limit) {
        Long min = offset == null || offset.equals(Query.NOLIMIT) ? 0 : offset;
        Long delta = limit == null || limit.equals(Query.NOLIMIT) ? null : limit;
        Long max = delta == null ? null : min + delta;

        Range<Long> result = max == null
                ? Range.atLeast(min)
                : Range.closedOpen(min, max);

        return result;
    }
}
