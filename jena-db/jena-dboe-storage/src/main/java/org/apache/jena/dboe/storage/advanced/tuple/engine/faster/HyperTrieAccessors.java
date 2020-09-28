package org.apache.jena.dboe.storage.advanced.tuple.engine.faster;

import java.util.Arrays;
import java.util.List;

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeLeafSet;
import org.apache.jena.ext.com.google.common.graph.Traverser;

public class HyperTrieAccessors {

    public static int getMaxComponentIdx(StorageNode<?, ?, ?> storageNode) {
        int maxSeenComponentIdx = -1;
        for (StorageNode<?, ?, ?> node : Traverser.<StorageNode<?, ?, ?>>forTree(StorageNode::getChildren).depthFirstPostOrder(storageNode)) {
            int[] idxs = node.getKeyTupleIdxs();
            for (int idx : idxs) {
                if (idx > maxSeenComponentIdx) {
                    maxSeenComponentIdx = idx;
                }
            }
        }

        return maxSeenComponentIdx;
    }

    public static <C> HyperTrieAccessor<C> index(StorageNode<?, C, ?> storageNode) {
        int tupleDim = getMaxComponentIdx(storageNode) + 1;

        HyperTrieAccessor<C> result = index(tupleDim, storageNode);

        return result;
//        if (storageNode.isAltNode()) {
//
//            int dim = children.size();
//            StorageNode<?, C, ?>[] componentIdxToChildIdx = (StorageNode<?, C, ?>[])new Object[dim];
//        }
    }

    public static <C> HyperTrieAccessor<C> index(int tupleDim, StorageNode<?, C, ?> storageNode) {
        HyperTrieAccessor<C> result;

        if (storageNode.isSetNode()) {
            @SuppressWarnings("unchecked")
            StorageNodeLeafSet<?, C, ?> leafStorageNode = (StorageNodeLeafSet<?, C, ?>)storageNode;
            result = new HyperTrieAccessorLeafSet<>(leafStorageNode);
        } else if (storageNode.isDelegate()) {
            result = index(tupleDim, storageNode.getPublicDelegate());
        } else {
            // StorageNode<?, C, ?>[] componentIdxToChildIdx = (StorageNode<?, C, ?>[])new Object[tupleDim];
            int[] componentIdxToChildIdx = new int[tupleDim];
            Arrays.fill(componentIdxToChildIdx, -1);

            @SuppressWarnings("unchecked")
            HyperTrieAccessor<C>[] componentIdxToChildAccessor = (HyperTrieAccessor<C>[])new HyperTrieAccessor[tupleDim];

            List<? extends StorageNode<?, C, ?>> children = storageNode.getChildren();

            for(int i = 0; i < children.size(); ++i) {
                StorageNode<?, C, ?> child = children.get(i);

                int[] tupleIdxs = child.getKeyTupleIdxs();

                if (tupleIdxs.length != 1) {
                    throw new IllegalArgumentException("Argument does not represent a valid tree; storage nodes may only index a single component; got: " + Arrays.toString(tupleIdxs) + " on " + storageNode.getClass());
                }

                int componentIdx = tupleIdxs[0];

                StorageNode<?, C, ?> mapOrLeaf = child.getChildren().get(0);

                HyperTrieAccessor<C> childAccessor = index(tupleDim, mapOrLeaf);

                componentIdxToChildIdx[componentIdx] = i;
                componentIdxToChildAccessor[componentIdx] = childAccessor; //child;
            }

            result = new HyperTrieAccessorAltOfInnerMaps<C>(storageNode, componentIdxToChildIdx, componentIdxToChildAccessor);
        }

        return result;
    }


//    public <C> HyperTrieAccessor<C> indexDepthFirstPostOrder(int dim, StorageNode<?, C, ?> storageNode) {
//        int[] componentIdxs = storageNode.getKeyTupleIdxs();
//        int componentIdx = componentIdxs[0];
//
//        if (storageNode.isSetNode()) {
//            componentToChildMap =
//        }
//    }


}
