package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.List;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.NodeStats;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.StoreAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.StoreAccessorImpl;
import org.apache.jena.dboe.storage.advanced.tuple.analysis.TupleQueryAnalyzer;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeMutable;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamer;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamerBinder;

/**
 *
 *
 * @author raven
 *
 * @param <D> Domain tuple type such as Triple, Quad, Tuple, List, etc
 * @param <C> Component type such as Node
 * @param <V> Store type such as a Map<Node, Set<Triple>>
 */
public abstract class TupleTableFromStorageNodeBase<D, C, V>
    implements TupleTableCoreFromStorageNode<D, C, V>
{
    protected StorageNodeMutable<D, C, V> rootStorageNode;
    protected V store;

    protected StoreAccessor<D, C> storeAccessor;

    public TupleTableFromStorageNodeBase(
            StorageNodeMutable<D, C, V> rootStorageNode,
            V store) {
        super();
        this.rootStorageNode = rootStorageNode;
        this.store = store;

        storeAccessor = StoreAccessorImpl.createForStorage(rootStorageNode);
    }

    @Override
    public StorageNodeMutable<D, C, V> getStorageNode() {
        return rootStorageNode;
    }

    @Override
    public V getStore() {
        return store;
    }

    @Override
    public void clear() {
        rootStorageNode.clear(store);
    }

    @Override
    public void add(D domainTuple) {
        rootStorageNode.add(store, domainTuple);
    }

    @Override
    public void delete(D domainTuple) {
        rootStorageNode.remove(store, domainTuple);
    }

    @Override
    public Stream<D> findTuples(List<C> pattern) {
        TupleFinder<D, D, C> finder = newFinder();
        for (int i = 0; i < pattern.size(); ++i) {
            finder.eq(i, pattern.get(i));
        }

        return finder.stream();
    }

    @Override
    public ResultStreamer<D, C, Tuple<C>> find(TupleQuery<C> tupleQuery) {
        NodeStats<D, C> bestMatch = TupleQueryAnalyzer.analyze(tupleQuery, storeAccessor);
        ResultStreamerBinder<D, C, Tuple<C>> binder = TupleQueryAnalyzer.createResultStreamer(
                bestMatch,
                tupleQuery,
                getTupleAccessor());

        return binder.bind(store);
    }
}
