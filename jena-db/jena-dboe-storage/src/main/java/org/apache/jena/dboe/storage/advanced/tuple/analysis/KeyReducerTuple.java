package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.ext.com.google.common.collect.HashMultimap;
import org.apache.jena.ext.com.google.common.collect.Multimap;

import com.github.andrewoma.dexx.collection.LinkedLists;

public class KeyReducerTuple<C>
    implements KeyReducer<com.github.andrewoma.dexx.collection.List<C>>
{

    protected int[] projection;
    protected List<KeyReducer2<com.github.andrewoma.dexx.collection.List<C>>> reducers = new ArrayList<>();
    protected Multimap<Integer, Integer> tupleIdxToProjSlots;
    protected int[] reducedKeyIdxToTupleIdx;
//    protected Function<? super com.github.andrewoma.dexx.collection.List<C>, ? extends Tuple<C>> tupleFactory;

    public KeyReducerTuple(
            int[] projection,
            List<KeyReducer2<com.github.andrewoma.dexx.collection.List<C>>> reducers,
            Multimap<Integer, Integer> tupleIdxToProjSlots,
            int[] reducedKeyIdxToTupleIdx) {
        super();
        this.projection = projection;
        this.reducers = reducers;
        this.tupleIdxToProjSlots = tupleIdxToProjSlots;
        this.reducedKeyIdxToTupleIdx = reducedKeyIdxToTupleIdx;

//        this.tupleFactory = tupleFactory;
    }


    public com.github.andrewoma.dexx.collection.List<C> newAccumulator() {
        return LinkedLists.of();
    }

    @SuppressWarnings("unchecked")
    public Tuple<C> makeTuple(com.github.andrewoma.dexx.collection.List<C> list) {
        Object tmp[] = new Object[projection.length];

        int[] i = {0};
        list.forEach(item -> {
            int tupleIdx = reducedKeyIdxToTupleIdx[i[0]];
            for(int slot : tupleIdxToProjSlots.get(tupleIdx)) {
                tmp[slot] = item;
            }

            ++i[0];
        });

        return (Tuple<C>) TupleFactory.create(tmp);
    }

    public static <C> Function<? super com.github.andrewoma.dexx.collection.List<C>, ? extends Tuple<C>> createTupleFactory(int length) {
        // Because we know the length of the tuples we could point directly to the ctor
        return list -> TupleFactory.create(list.asList());
//    	switch(length) {
//    	case
//
//    	};
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
        Set<Integer> remainingTupleIdxs = new HashSet<>();

        for (int i = 0; i < projection.length; ++i) {
            int tupleIdx = projection[i];
            tupleIdxToProjSlot.put(tupleIdx, i);

            // E.g. {2, 3, 9}
            remainingTupleIdxs.add(tupleIdx);
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
                if (remainingTupleIdxs.contains(tupleIdx)) {
                    reducedKeyIdxToTupleIdx.add(tupleIdx);
                    remainingTupleIdxs.remove(tupleIdx);
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

//        Function<? super com.github.andrewoma.dexx.collection.List<C>, ? extends Tuple<C>> tupleFactory = createTupleFactory(projection.length);

        return new KeyReducerTuple<C>(
                projection,
                reducers, tupleIdxToProjSlot,
                reducedKeyIdxToTupleIdx.stream().mapToInt(x -> x).toArray());
    }

}
