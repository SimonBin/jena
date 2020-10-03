package org.apache.jena.dboe.storage.advanced.tuple.engine.faster;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.dboe.storage.advanced.tuple.engine.SliceNode;
import org.apache.jena.ext.com.google.common.collect.Sets;

public class SliceNode2Accessor<C> {
    protected HyperTrieAccessor<C> storeAccessor;

    protected int[] remainingVarIdxs;
    protected int[][] varIdxToTupleIdxs;


    public SliceNode2Accessor(HyperTrieAccessor<C> storeAccessor, int[] remainingVarIdxs,
            int[][] varIdxToTupleIdxs) {
        super();
        this.storeAccessor = storeAccessor;
        this.remainingVarIdxs = remainingVarIdxs;
        this.varIdxToTupleIdxs = varIdxToTupleIdxs;
    }


    public Set<C> getValuesForComponent(Object store, int tupleIdx) {
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


    public Set<C> getSmallestValueSetForVarIdx(Object store, int varIdx) {
        Set<C> result = null;
        int tupleIdxs[] = varIdxToTupleIdxs[varIdx];

        for (int tupleIdx : tupleIdxs) {
            Set<C> candidate = getValuesForComponent(store, tupleIdx);
            result = result == null
                    ? candidate
                    : (candidate.size() < result.size() ? candidate : result);
        }


        return result;
    }

    public Set<C> getLargestValueSetForVarIdx(Object store, int varIdx) {
        Set<C> result = null;
        int tupleIdxs[] = varIdxToTupleIdxs[varIdx];

        for (int tupleIdx : tupleIdxs) {
            Set<C> candidate = getValuesForComponent(store, tupleIdx);
            result = result == null
                    ? candidate
                    : (candidate.size() > result.size() ? candidate : result);
        }


        return result;
    }

    public interface Slicer<C> {
        Object apply(Object store, C value);
        SliceNode2Accessor<C> subStoreAccessor();
        boolean isSingleComponent();

        // Only value if isSingleComponent is true
        Set<C> values(Object store);

        Set<C>[] valueSets(Object store);

    }

    public class SlicerSimple
        implements Slicer<C>
    {
        protected int bestMatchTupleIdx;
        protected int[] nextRemainingVarIdxs;
        protected HyperTrieAccessor<C> nextStoreAccessor;

        protected SliceNode2Accessor<C> nextSliceAccessor;


        public SlicerSimple(HyperTrieAccessor<C> nextStoreAccessor, int bestMatchTupleIdx, int[] nextRemainingVarIdxs) {
            super();
            this.bestMatchTupleIdx = bestMatchTupleIdx;
            this.nextRemainingVarIdxs = nextRemainingVarIdxs;
            this.nextStoreAccessor = nextStoreAccessor;
            this.nextSliceAccessor = new SliceNode2Accessor<>(nextStoreAccessor, nextRemainingVarIdxs, varIdxToTupleIdxs);
        }


        @Override
        public SliceNode2Accessor<C> subStoreAccessor() {
            return nextSliceAccessor;
        }

        @Override
        public Object apply(Object store, C value) {
            Object result = storeAccessor.getStoreForSliceByComponentByValue(store, bestMatchTupleIdx, value);
//            SliceNode2<C> result = null;
//            if (subStore != null) {
//                HyperTrieAccessor<C> subAccessor = storeAccessor.getAccessorForComponent(bestMatchTupleIdx);
//                result = new SliceNode2<>(subStore, subAccessor, nextRemainingVarIdxs, varIdxToTupleIdxs);
//            }
            return result;
        }


        @Override
        public Set<C> values(Object store) {
            Set<C> result = storeAccessor.getValuesForComponent(store, bestMatchTupleIdx);
            return result;
        }


        @Override
        public boolean isSingleComponent() {
            return true;
        }


        @Override
        public Set<C>[] valueSets(Object store) {
            @SuppressWarnings("unchecked")
            Set<C>[] result = new Set[] { values(store) };
            return result;
        }
    }

    /**
     * Slicer for the case where a variable maps to multiple components
     *
     * @author raven
     *
     */
    public class SlicerComplex
        implements Slicer<C>
    {
        protected int[] tupleIdxs;
        protected int[] nextRemainingVarIdxs;
        protected HyperTrieAccessor<C> nextStoreAccessor;

        protected SliceNode2Accessor<C> nextSliceAccessor;


        public SlicerComplex(HyperTrieAccessor<C> nextStoreAccessor, int[] tupleIdxs, int[] nextRemainingVarIdxs) {
            super();
            this.tupleIdxs = tupleIdxs;
            this.nextRemainingVarIdxs = nextRemainingVarIdxs;
            this.nextStoreAccessor = nextStoreAccessor;
            this.nextSliceAccessor = new SliceNode2Accessor<>(nextStoreAccessor, nextRemainingVarIdxs, varIdxToTupleIdxs);
        }


        @Override
        public SliceNode2Accessor<C> subStoreAccessor() {
            return nextSliceAccessor;
        }

        public Set<C> values(Object store) {
            Set<C>[] sets = valueSets(store);
            Arrays.sort(sets, (a, b) -> a.size() - b.size());
            Set<C> result = sets[0];

            int l = sets.length;
            for (int i = 1; i < l; ++i) {
                Set<C> contrib = sets[i];
                result = Sets.intersection(result, contrib);
            }

            return result;
        }

        @Override
        public Object apply(Object store, C value) {
            int bestMatchTupleIdx = tupleIdxs[0];
            if (tupleIdxs.length > 1) {
                int bestMatchSize = 0;
                for (int tupleIdx : tupleIdxs) {
                    Set<?> values = getValuesForComponent(store, tupleIdx);
                    int valuesSize = values.size();
                    if(valuesSize > bestMatchSize) {
                        bestMatchSize = valuesSize;
                        bestMatchTupleIdx = tupleIdx;
                    }
                }
            }

            Object result = storeAccessor.getStoreForSliceByComponentByValue(store, bestMatchTupleIdx, value);
//            SliceNode2<C> result = null;
//            if (subStore != null) {
//                HyperTrieAccessor<C> subAccessor = storeAccessor.getAccessorForComponent(bestMatchTupleIdx);
//                result = new SliceNode2<>(subStore, subAccessor, nextRemainingVarIdxs, varIdxToTupleIdxs);
//            }
            return result;
        }


        @Override
        public boolean isSingleComponent() {
            return false;
        }


        @Override
        public Set<C>[] valueSets(Object store) {
            int l = tupleIdxs.length;
            @SuppressWarnings("unchecked")
            Set<C>[] result = new Set[l];
            for (int i = 0; i < l; ++i) {
                result[i] = getValuesForComponent(store, tupleIdxs[i]);
            }
            return result;
        }
    }



    public Slicer slicerForComponentIdx(int componentIdx) {
        int[] nextRemainingVarIdxs = SliceNode.removeRemainingVarByTupleIdx(componentIdx, remainingVarIdxs, varIdxToTupleIdxs);

        HyperTrieAccessor<C> nextStoreAccessor = storeAccessor.getAccessorForComponent(componentIdx);

        Slicer result = new SlicerSimple(nextStoreAccessor, componentIdx, nextRemainingVarIdxs);
        return result;
    }


    public Slicer slicerForVarIdx(int varIdx) {
        assert remainingVarIdxs.length != 0;
        assert hasRemainingVarIdx(varIdx);

        int[] nextRemainingVarIdxs = ArrayUtils.removeElement(remainingVarIdxs, varIdx);
        int[] tupleIdxs = varIdxToTupleIdxs[varIdx];

        Slicer result = tupleIdxs == null
                ? null
                : tupleIdxs.length != 1
                    ? new SlicerComplex(storeAccessor.getAccessorForComponent(tupleIdxs[0]), tupleIdxs, nextRemainingVarIdxs)
                    : slicerForComponentIdx(tupleIdxs[0]);

        return result;

    }

}
