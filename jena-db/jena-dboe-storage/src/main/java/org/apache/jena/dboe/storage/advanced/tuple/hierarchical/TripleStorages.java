package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.alt2;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.alt3;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.innerMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafComponentSet;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.wrapInsert;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTriple;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;

public class TripleStorages {

    /**
     * Create a conventional storage with SPO, OPS and POS index
     *
     * @returns
     */
    public static StorageNodeMutable<Triple, Node, ?> createConventionalStorage() {
        StorageNodeMutable<Triple, Node, ?> storage =
                alt3(
                    // spo
                    innerMap(0, HashMap::new,
                        innerMap(1, HashMap::new,
                            leafMap(2, HashMap::new, TupleAccessorTriple.INSTANCE)))
                    ,
                    // ops
                    innerMap(2, HashMap::new,
                        innerMap(1, HashMap::new,
                            leafMap(0, HashMap::new, TupleAccessorTriple.INSTANCE)))
                    ,
                    // pos
                    innerMap(1, HashMap::new,
                        innerMap(2, HashMap::new,
                            leafMap(0, HashMap::new, TupleAccessorTriple.INSTANCE)))
                );

        return storage;
    }

    /**
     * A storage that indexes triples in a nested structure in all possible ways:
     * (S(P(O)|O(P)) | P(S(O)|O(S)) | O(P(S)|S(P)))
     *
     * @return
     */
    public static StorageNodeMutable<int[], Integer, ?> createHyperTrieStorageInt(
            TupleAccessor<int[], Integer> accessor) {

        // TODO The leaf set suppliers should be invoked with context information such that
        // different collation orders of the component indices can yield the same set
        // E.g. 1=rdf:type 2=foaf:Person can reuse the set for 2=foaf:Person 1=rdf:type

        StorageNodeMutable<int[], Integer,
            Alt3<
                Map<Integer, Alt2<Map<Integer, Set<Integer>>, Map<Integer, Set<Integer>>>>,
                Map<Integer, Alt2<Map<Integer, Set<Integer>>, Map<Integer, Set<Integer>>>>,
                Map<Integer, Alt2<Map<Integer, Set<Integer>>, Map<Integer, Set<Integer>>>>>
            >
        result = alt3(
            innerMap(0, Int2ObjectOpenHashMap::new, alt2(
                innerMap(1, Int2ObjectOpenHashMap::new, leafComponentSet(2, SetSupplier.force(IntArraySet::new), accessor)),
                innerMap(2, Int2ObjectOpenHashMap::new, leafComponentSet(1, SetSupplier.force(IntArraySet::new), accessor)))),
            innerMap(1, HashMap::new, alt2(
                innerMap(0, Int2ObjectOpenHashMap::new, leafComponentSet(2, SetSupplier.force(IntArraySet::new), accessor)),
                innerMap(2, Int2ObjectOpenHashMap::new, leafComponentSet(0, SetSupplier.force(IntArraySet::new), accessor)))),
            innerMap(2, HashMap::new, alt2(
                innerMap(0, Int2ObjectOpenHashMap::new, leafComponentSet(1, SetSupplier.force(IntArraySet::new), accessor)),
                innerMap(1, Int2ObjectOpenHashMap::new, leafComponentSet(0, SetSupplier.force(IntArraySet::new), accessor))))
        );

        return result;
    }

    /**
     * A storage that indexes triples in a nested structure in all possible ways:
     * (S(P(O)|O(P)) | P(S(O)|O(S)) | O(P(S)|S(P)))
     *
     * @return
     */
    public static <D, C> StorageNodeMutable<D, C, ?> createHyperTrieStorage(TupleAccessor<D, C> accessor) {
        //TupleAccessor<Triple, Node> accessor = TupleAccessorTripleAnyToNull.INSTANCE;

        // TODO The leaf set suppliers should be invoked with context information such that
        // different collation orders of the component indices can yield the same set
        // E.g. 1=rdf:type 2=foaf:Person can reuse the set for 2=foaf:Person 1=rdf:type

        // go through the permutations of tuple components in order and check
        // whether for the last component there was already a prior entry with permutated parents
        // Example: For ps(o) we check whether we already encounteered a sp(o)
        // spo
        // sop
        // pso -> spo
        // pos
        // osp -> sop
        // ops -> pos

        SetSupplier spo = HashSet::new;
        SetSupplier sop = HashSet::new;
        SetSupplier pos = HashSet::new;

        SetSupplier pso = SetSupplier.none(); // reuses spo
        SetSupplier osp = SetSupplier.none(); // reuses sop
        SetSupplier ops = SetSupplier.none(); // reuses pos

        StorageNodeMutable<D, C,
            Alt3<
                Map<C, Alt2<Map<C, Set<C>>, Map<C, Set<C>>>>,
                Map<C, Alt2<Map<C, Set<C>>, Map<C, Set<C>>>>,
                Map<C, Alt2<Map<C, Set<C>>, Map<C, Set<C>>>>>
            >
        result = wrapInsert(alt3(
            innerMap(0, HashMap::new, alt2(
                innerMap(1, HashMap::new, leafComponentSet(2, spo, accessor)),
                innerMap(2, HashMap::new, leafComponentSet(1, sop, accessor)))),
            innerMap(1, HashMap::new, alt2(
                innerMap(0, HashMap::new, leafComponentSet(2, pso, accessor)),
                innerMap(2, HashMap::new, leafComponentSet(0, pos, accessor)))),
            innerMap(2, HashMap::new, alt2(
                innerMap(0, HashMap::new, leafComponentSet(1, osp, accessor)),
                innerMap(1, HashMap::new, leafComponentSet(0, ops, accessor))))
        ), (store, tup) -> {
            C s = accessor.get(tup, 0);
            C p = accessor.get(tup, 1);
            C o = accessor.get(tup, 2);

            Alt2<Map<C, Set<C>>, Map<C, Set<C>>> sm = store.getV1().get(s);
            Set<C> spom = sm.getV1().get(p);
            Set<C> sopm = sm.getV2().get(o);

            Alt2<Map<C, Set<C>>, Map<C, Set<C>>> pm = store.getV2().get(p);
            /* Set<C> psom = */ pm.getV1().put(s, spom);
            Set<C> posm = pm.getV2().get(o);

            Alt2<Map<C, Set<C>>, Map<C, Set<C>>> om = store.getV3().get(o);
            /* Set<C> ospm = */ om.getV1().put(s, sopm);
            /* Set<C> opsm = */ om.getV2().put(p, posm);

        });



        return result;
    }


    // Let's recap
    // [3::] [:2:] == [:2:]*[3::]

    /*
    onInsert(tuple, accessor) {

    }

    Set<C> find(int idx, tuple, accessor)

    */

    public static MapSupplier reuse(int... idxs) {
        return null;
    }
}
