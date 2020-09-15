package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;

import com.github.andrewoma.dexx.collection.List;

public class KeyReducerList<C>
    implements KeyReducer2<List<C>>
{
    protected StoreAccessor<?, C> accessor;
    protected StorageNode<?, C, ?> storage;
    protected int[] keyComponentsToAppend; // indexes pointing into storage.getKeyTupleIdxs()

    public KeyReducerList(
            StoreAccessor<?, C> accessor, int[] keyComponentsToAppend) {
        super();
        this.accessor = accessor;
        this.storage = accessor.getStorage();
        this.keyComponentsToAppend = keyComponentsToAppend;
    }

    @Override
    public List<C> reduce(List<C> accumulator, Object key) {
        for (int i = 0; i < keyComponentsToAppend.length; ++i) {
            int componentIdx = keyComponentsToAppend[i];

            C value = storage.getKeyComponentRaw(key, componentIdx);
            accumulator = accumulator.append(value);
        }

        return accumulator;
    }

}
