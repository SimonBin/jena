package org.apache.jena.dboe.storage.advanced.tuple.engine.faster;

import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.dboe.storage.advanced.tuple.engine.SliceNode;


public class SliceNode2<C> {
    protected SliceNode2Accessor<C> sliceAccessor;
    protected Object store;

    public SliceNode2(SliceNode2Accessor<C> sliceAccessor, Object store) {
        super();
        this.sliceAccessor = sliceAccessor;
        this.store = store;
    }

    public SliceNode2Accessor<C> getSliceAccessor() {
        return sliceAccessor;
    }

    public Object getStore() {
        return store;
    }

    public static <C> SliceNode2<C> create(Object store, HyperTrieAccessor<C> storeAccessor, int[] remainingVarIdxs,
            int[][] varIdxToTupleIdxs) {
        return new SliceNode2<>(new SliceNode2Accessor<C>(storeAccessor, remainingVarIdxs, varIdxToTupleIdxs), store);
    }


    public  Set<C> getValuesForComponent(int tupleIdx) {
        Set<C> result = sliceAccessor.getValuesForComponent(store, tupleIdx);
        return result;
    }


    public int[] getRemainingVars() {
        return sliceAccessor.getRemainingVars();
    }

    public int[][] getVarIdxToTupleIdxs() {
        return sliceAccessor.getVarIdxToTupleIdxs();
    }

    public boolean hasRemainingVarIdx(int varIdx) {
        return sliceAccessor.hasRemainingVarIdx(varIdx);
    }

    public Set<C> getSmallestValueSetForVarIdx(int varIdx) {
        Set<C> result = sliceAccessor.getSmallestValueSetForVarIdx(store, varIdx);
        return result;
    }

    public Set<C> getLargestValueSetForVarIdx(int varIdx) {
        Set<C> result = sliceAccessor.getLargestValueSetForVarIdx(store, varIdx);
        return result;
    }

    public class Slicer
    {
        protected SliceNode2Accessor.Slicer<C> slicer;

        public Slicer(SliceNode2Accessor.Slicer<C> slicer) {
            super();
            this.slicer = slicer;
        }

        public SliceNode2<C> apply(C value) {
            Object subStore = slicer.apply(store, value);

            SliceNode2<C> result = null;
            if (subStore != null) {
                SliceNode2Accessor<C> subAccessor = slicer.subStoreAccessor();// storeAccessor.getAccessorForComponent(bestMatchTupleIdx);
                result = new SliceNode2<>(subAccessor, subStore);
            }
            return result;
        }
    }

    public Slicer slicerForComponentIdx(int componentIdx) {
        SliceNode2Accessor.Slicer<C> slicer = sliceAccessor.slicerForComponentIdx(componentIdx);
        return new Slicer(slicer);
    }

    public Slicer slicerForVarIdx(int varIdx) {
        SliceNode2Accessor.Slicer<C> slicer = sliceAccessor.slicerForVarIdx(varIdx);
        Slicer result = slicer == null
                ? null
                : new Slicer(slicer);

        return result;
    }
}


class SliceNode2Old<C> {
    protected Object store;
    protected HyperTrieAccessor<C> storeAccessor;

    protected int[] remainingVarIdxs;
    protected int[][] varIdxToTupleIdxs;


    public SliceNode2Old(Object store, HyperTrieAccessor<C> storeAccessor, int[] remainingVarIdxs,
            int[][] varIdxToTupleIdxs) {
        super();
        this.store = store;
        this.storeAccessor = storeAccessor;
        this.remainingVarIdxs = remainingVarIdxs;
        this.varIdxToTupleIdxs = varIdxToTupleIdxs;
    }


    public  Set<C> getValuesForComponent(int tupleIdx) {
//        Object subStore = storeAccessor.getStoreForSliceByComponentByValue(store, 0, NodeFactory.createURI("http://www.example.org/e4"));
//        HyperTrieAccessor<C> sp = storeAccessor.getAccessorForComponent(tupleIdx);
        Set<C> result = storeAccessor.getValuesForComponent(store, tupleIdx);
        return result;
    }


    public int[] getRemainingVars() {
        return remainingVarIdxs;
    }

    public int[][] getVarIdxToTupleIdxs() {
        return varIdxToTupleIdxs;
    }

    public boolean hasRemainingVarIdx(int varIdx) {
        return ArrayUtils.contains(remainingVarIdxs, varIdx);
    }

    public Set<C> getSmallestValueSetForVarIdx(int varIdx) {
        Set<C> result = null;
        int tupleIdxs[] = varIdxToTupleIdxs[varIdx];

        for (int tupleIdx : tupleIdxs) {
            Set<C> candidate = getValuesForComponent(tupleIdx);
            result = result == null
                    ? candidate
                    : (candidate.size() < result.size() ? candidate : result);
        }


        return result;
    }

    public Set<C> getLargestValueSetForVarIdx(int varIdx) {
        Set<C> result = null;
        int tupleIdxs[] = varIdxToTupleIdxs[varIdx];

        for (int tupleIdx : tupleIdxs) {
            Set<C> candidate = getValuesForComponent(tupleIdx);
            result = result == null
                    ? candidate
                    : (candidate.size() > result.size() ? candidate : result);
        }


        return result;
    }

    public class Slicer
    {
        protected int bestMatchTupleIdx;
        protected int[] nextRemainingVarIdxs;



        public Slicer(int bestMatchTupleIdx, int[] nextRemainingVarIdxs) {
            super();
            this.bestMatchTupleIdx = bestMatchTupleIdx;
            this.nextRemainingVarIdxs = nextRemainingVarIdxs;
        }



        public SliceNode2Old<C> apply(C value) {
            Object subStore = storeAccessor.getStoreForSliceByComponentByValue(store, bestMatchTupleIdx, value);
            SliceNode2Old<C> result = null;
            if (subStore != null) {
                HyperTrieAccessor<C> subAccessor = storeAccessor.getAccessorForComponent(bestMatchTupleIdx);
                result = new SliceNode2Old<>(subStore, subAccessor, nextRemainingVarIdxs, varIdxToTupleIdxs);
            }
            return result;
        }
    }

    public Slicer slicerForComponentIdx(int componentIdx) {
        int[] nextRemainingVarIdxs = SliceNode.removeRemainingVarByTupleIdx(componentIdx, remainingVarIdxs, varIdxToTupleIdxs);

        Slicer result = new Slicer(componentIdx, nextRemainingVarIdxs);
        return result;
    }

    public Slicer slicerForVarIdx(int varIdx) {
        assert remainingVarIdxs.length != 0;
        assert hasRemainingVarIdx(varIdx);

        Slicer result;
        int[] tupleIdxs = varIdxToTupleIdxs[varIdx];

        if (tupleIdxs != null) {
            // Slice by the component with fewer immediate remaining values (immediate means that we do not
            // count the values on the leaf nodes)
            int bestMatchTupleIdx = tupleIdxs[0];
            if (tupleIdxs.length > 1) {
                int bestMatchSize = 0;
                for (int tupleIdx : tupleIdxs) {
                    Set<?> values = getValuesForComponent(tupleIdx);
                    int valuesSize = values.size();
                    if(valuesSize > bestMatchSize) {
                        bestMatchSize = valuesSize;
                        bestMatchTupleIdx = tupleIdx;
                    }
                }
            }

            int[] nextRemainingVarIdxs = ArrayUtils.removeElement(remainingVarIdxs, varIdx);


            result = new Slicer(bestMatchTupleIdx, nextRemainingVarIdxs);//sliceOnComponentWithValue(bestMatchTupleIdx, value);
        } else {
            result = null;
        }
        return result;

    }


}
