package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

public class Meta2NodePseudo<D, C, V>
    extends Meta2NodeNoKeyBase<D, C, V>
{

    public Meta2NodePseudo(TupleAccessor<D, C> tupleAccessor) {
        super(tupleAccessor);
    }

    @Override
    public List<? extends StorageNode<D, C, ?>> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public Object chooseSubStore(V store, int subStoreIdx) {
        throw new UnsupportedOperationException("pseudo stores do not have a sub store");
    }

    @Override
    public <T> Stream<?> streamEntries(V store, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor) {
        return null;
    }

    @Override
    public <T> Streamer<V, ? extends Entry<?, ?>> streamerForKeyAndSubStoreAlts(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return null;
    }

}
