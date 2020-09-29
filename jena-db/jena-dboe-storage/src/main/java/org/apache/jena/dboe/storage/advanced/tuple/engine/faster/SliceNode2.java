package org.apache.jena.dboe.storage.advanced.tuple.engine.faster;

import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.dboe.storage.advanced.tuple.engine.SliceNode;

public class SliceNode2<C> {
    protected Object store;
    protected HyperTrieAccessor<C> storeAccessor;

    protected int[] remainingVarIdxs;
    protected int[][] varIdxToTupleIdxs;


    public SliceNode2(Object store, HyperTrieAccessor<C> storeAccessor, int[] remainingVarIdxs,
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



        public SliceNode2<C> apply(C value) {
            Object subStore = storeAccessor.getStoreForSliceByComponentByValue(store, bestMatchTupleIdx, value);
            SliceNode2<C> result = null;
            if (subStore != null) {
                HyperTrieAccessor<C> subAccessor = storeAccessor.getAccessorForComponent(bestMatchTupleIdx);
                result = new SliceNode2<>(subStore, subAccessor, nextRemainingVarIdxs, varIdxToTupleIdxs);
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
