package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.List;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

/**
 * Base class for index nodes that do not index by a key - or rather:
 * index by a single key that is a zero-sized tuple
 *
 * @author raven
 *
 * @param <D>
 * @param <C>
 * @param <V>
 */
public class Meta2NodeNoKeyBase<D, C, V>
    extends Meta2NodeBase<D, C, V>
{
    public Meta2NodeNoKeyBase(TupleAccessor<D, C> tupleAccessor) {
        super(new int[] {}, tupleAccessor);
    }

    @Override
    public List<? extends Meta2Node<D, C, ?>> getChildren() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> Streamer<V, C> streamerForKeysAsComponent(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        throw new UnsupportedOperationException("Cannot stream keys as components if there are no keys");
    }

    @Override
    public <T> Streamer<V, Tuple<C>> streamerForKeysAsTuples(T pattern,
            TupleAccessorCore<? super T, ? extends C> accessor) {
        return argStore -> Stream.of(TupleFactory.create0());
    }

    @Override
    public <T> Stream<?> streamEntries(V store, T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor) {
        // TODO Auto-generated method stub
        return null;
    }
}
