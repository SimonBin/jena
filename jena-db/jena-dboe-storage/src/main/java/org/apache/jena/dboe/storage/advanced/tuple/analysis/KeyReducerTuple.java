package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.ext.com.google.common.collect.HashMultimap;
import org.apache.jena.ext.com.google.common.collect.Multimap;

import com.github.andrewoma.dexx.collection.LinkedLists;

public class KeyReducerTuple<C>
    implements KeyReducer<com.github.andrewoma.dexx.collection.List<C>>
{

    protected List<KeyReducer2<com.github.andrewoma.dexx.collection.List<C>>> reducers = new ArrayList<>();
    protected Multimap<Integer, Integer> tupleIdxToProjSlot;
    protected List<Integer> reducedKeyIdxToTupleIdx;

    public KeyReducerTuple(
            List<KeyReducer2<com.github.andrewoma.dexx.collection.List<C>>> reducers,
            Multimap<Integer, Integer> tupleIdxToProjSlot,
            List<Integer> reducedKeyIdxToTupleIdx
            ) {
        super();
        this.reducers = reducers;
        this.tupleIdxToProjSlot = tupleIdxToProjSlot;
        this.reducedKeyIdxToTupleIdx = reducedKeyIdxToTupleIdx;
    }


    public com.github.andrewoma.dexx.collection.List<C> newAccumulator() {
        return LinkedLists.of();
    }

    @Override
    public com.github.andrewoma.dexx.collection.List<C> reduce(
            com.github.andrewoma.dexx.collection.List<C> accumulator,
            int indexNode,
            Object value) {
        KeyReducer2<com.github.andrewoma.dexx.collection.List<C>> reducer = reducers.get(indexNode);
        com.github.andrewoma.dexx.collection.List<C> result = reducer.reduce(accumulator, value);
        return result;
    }



    /**
     * Projection that tracks all extracted components of relevant keys in a persistent list
     *
     * @param <C>
     * @param accessorNode
     * @param projection The projected tuple indexes
     */
    public static <C> KeyReducerTuple<C> createForProjection(StoreAccessor<?, C> accessorNode, int[] projection) {

        List<KeyReducer2<com.github.andrewoma.dexx.collection.List<C>>> reducers = new ArrayList<>();


        // Inverted projection which maps projected tuple ids to the slots
        // e.g. [3, 2, 3, 9] becomes [2: {1}, 3: {0, 1}, 9:{3}]
        Multimap<Integer, Integer> tupleIdxToProjSlot = HashMultimap.create();
        Set<Integer> remainingProjIdx = new HashSet<>();

        for (int i = 0; i < projection.length; ++i) {
            int tupleIdx = projection[i];
            tupleIdxToProjSlot.put(tupleIdx, i);

            // E.g. {2, 3, 9}
            remainingProjIdx.add(tupleIdx);
        }


        // Mapping the indexes of the reduced list to the slots in the projection they satisfies
        // E.g. if the projection is [3, 4, 9] and an index can satisfy [9] then
        // it states that it can map to the slot with index 2 in the projection
        List<Integer> reducedKeyIdxToTupleIdx = new ArrayList<>(projection.length);

        List<? extends StoreAccessor<?, C>> ancestors = accessorNode.ancestors();

        for (int i = 0; i < ancestors.size(); ++i) {
            StoreAccessor<?, C> node = ancestors.get(i);

            List<Integer> keyComponentIdxsToAppend = new ArrayList<>();

            int[] nodeTupleIdxs = node.getStorage().getKeyTupleIdxs();

            // Check if any indexes of the tuple indices appear in the array
            for (int t = 0; t < nodeTupleIdxs.length; ++t) {
                int tupleIdx = nodeTupleIdxs[t];

                // Take note of any tuple idx that are requested and not already served
                // A reducer will be created for them that collects the component values into a list
                if (remainingProjIdx.contains(tupleIdx)) {
                    reducedKeyIdxToTupleIdx.add(tupleIdx);
                    remainingProjIdx.remove(tupleIdx);
                    keyComponentIdxsToAppend.add(t);
                }
            }

            // If none of the keys are relevant to the projection just pass on whatever components
            // we collected
            KeyReducer2<com.github.andrewoma.dexx.collection.List<C>> nodeReducer = !keyComponentIdxsToAppend.isEmpty()
                    ? new KeyReducerList<C>(node, keyComponentIdxsToAppend.stream().mapToInt(x -> x).toArray())
                    : KeyReducer2::passOn;

            reducers.add(nodeReducer);
        }


        return new KeyReducerTuple<C>(reducers, tupleIdxToProjSlot, reducedKeyIdxToTupleIdx);
    }

}
