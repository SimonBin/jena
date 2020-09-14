package org.apache.jena.dboe.storage.advanced.tuple.trash;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.apache.jena.ext.com.google.common.collect.Multimap;
import org.apache.jena.ext.com.google.common.collect.Multimaps;

public class IndexNodeForkRoot<ComponentType>
    extends IndexNodeForkBase<ComponentType>
{
    protected Supplier<Long> tupleTableSizeSupp;

    public IndexNodeForkRoot(Supplier<Long> tupleTableSizeSupp) {
        this(null, tupleTableSizeSupp);
    }

    public IndexNodeForkRoot(IndexNode<ComponentType> parent, Supplier<Long> tupleTableSizeSupp) {
        super(parent);
        this.tupleTableSizeSupp = tupleTableSizeSupp;
    }

    @Override
    public long estimateIndexSize() {
        long result = tupleTableSizeSupp.get();
        return result;
    }

    @Override
    public long estimateRemainingValueCount(ComponentType constraint) {
        long result = tupleTableSizeSupp.get();
        return result;
    }

    @Override
    public Multimap<List<Integer>, IndexNodeFactory<ComponentType>> choices() {
        return Multimaps.forMap(Collections.emptyMap());
    }
}
