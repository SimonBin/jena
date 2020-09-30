package org.apache.jena.dboe.storage.advanced.tuple.engine.faster;

import java.util.Map;
import java.util.Set;

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;


public class HyperTrieAccessorAltOfInnerMaps<C>
    implements HyperTrieAccessor<C>
{
    // A sparse array with the mapping of component to children
    // Unmapped entries should be set to -1 in order for early crashes on errors

    protected StorageNode<?, C, ?> storageAlt;

    protected int[] componentIdxToChildIdx;
    protected HyperTrieAccessor<C>[] childAccessors;


    public HyperTrieAccessorAltOfInnerMaps(
            StorageNode<?, C, ?> storageAlt,
            int[] componentIdxToChildIdx,
            HyperTrieAccessor<C>[] childAccessors
            ) {
        super();
        this.storageAlt = storageAlt;
        this.componentIdxToChildIdx = componentIdxToChildIdx;
        this.childAccessors = childAccessors;
    }


    @Override
    public HyperTrieAccessor<C> getAccessorForComponent(int idx) {
        return childAccessors[idx];
    }


//    public Object getSubStoreForComponent(Object storeAlt, int componentIdx) {
//        int subStoreIdx = componentIdxToChildIdx[componentIdx];
//        Object targetStore = storageAlt.chooseSubStoreRaw(storeAlt, subStoreIdx);
//
//        return targetStore;
//    }

    @Override
    public Object getStoreForSliceByComponentByValue(Object storeAlt, int componentIdx, C value) {
        int subStoreIdx = componentIdxToChildIdx[componentIdx];
        Object targetStore = storageAlt.chooseSubStoreRaw(storeAlt, subStoreIdx);

        // Return the map or set for the value
        StorageNode<?, C, ?> child = storageAlt.getChildren().get(subStoreIdx);
        Map<C, ?> map = (Map<C, ?>)child.getStoreAsMap(targetStore);

        Object result = map.get(value);

        return result;
    }


    @Override
    public Set<C> getValuesForComponent(Object storeAlt, int componentIdx) {
        int subStoreIdx = componentIdxToChildIdx[componentIdx];
        Object targetStore = storageAlt.chooseSubStoreRaw(storeAlt, subStoreIdx);

        // Return the map or set for the value
        StorageNode<?, C, ?> child = storageAlt.getChildren().get(subStoreIdx);
        Map<C, ?> map = (Map<C, ?>)child.getStoreAsMap(targetStore);

        Set<C> result = map.keySet();
        return result;
    }

}

