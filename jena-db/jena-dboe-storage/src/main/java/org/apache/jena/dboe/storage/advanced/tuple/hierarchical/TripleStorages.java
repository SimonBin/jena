package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.alt2;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.alt3;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.innerMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafComponentSet;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTriple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTripleAnyToNull;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

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
    public static StorageNodeMutable<Triple, Node, ?> createHyperTrieStorageRaw() {
        TupleAccessor<Triple, Node> accessor = TupleAccessorTripleAnyToNull.INSTANCE;

        // TODO The leaf set suppliers should be invoked with context information such that
        // different collation orders of the component indices can yield the same set
        // E.g. 1=rdf:type 2=foaf:Person can reuse the set for 2=foaf:Person 1=rdf:type

        StorageNodeMutable<Triple, Node,
            Alt3<
                Map<Node, Alt2<Map<Node, Set<Node>>, Map<Node, Set<Node>>>>,
                Map<Node, Alt2<Map<Node, Set<Node>>, Map<Node, Set<Node>>>>,
                Map<Node, Alt2<Map<Node, Set<Node>>, Map<Node, Set<Node>>>>>
            >
        result = alt3(
            innerMap(0, HashMap::new, alt2(
                innerMap(1, HashMap::new, leafComponentSet(2, HashSet::new, accessor)),
                innerMap(2, HashMap::new, leafComponentSet(1, HashSet::new, accessor)))),
            innerMap(1, HashMap::new, alt2(
                innerMap(0, HashMap::new, leafComponentSet(2, HashSet::new, accessor)),
                innerMap(2, HashMap::new, leafComponentSet(0, HashSet::new, accessor)))),
            innerMap(2, HashMap::new, alt2(
                innerMap(0, HashMap::new, leafComponentSet(1, HashSet::new, accessor)),
                innerMap(1, HashMap::new, leafComponentSet(0, HashSet::new, accessor))))
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


        StorageNodeMutable<D, C,
            Alt3<
                Map<C, Alt2<Map<C, Set<C>>, Map<C, Set<C>>>>,
                Map<C, Alt2<Map<C, Set<C>>, Map<C, Set<C>>>>,
                Map<C, Alt2<Map<C, Set<C>>, Map<C, Set<C>>>>>
            >
        result = alt3(
            innerMap(0, HashMap::new, alt2(
                innerMap(1, HashMap::new, leafComponentSet(2, HashSet::new, accessor)),
                innerMap(2, HashMap::new, leafComponentSet(1, HashSet::new, accessor)))),
            innerMap(1, HashMap::new, alt2(
                innerMap(0, HashMap::new, leafComponentSet(2, HashSet::new, accessor)),
                innerMap(2, HashMap::new, leafComponentSet(0, HashSet::new, accessor)))),
            innerMap(2, HashMap::new, alt2(
                innerMap(0, HashMap::new, leafComponentSet(1, HashSet::new, accessor)),
                innerMap(1, HashMap::new, leafComponentSet(0, HashSet::new, accessor))))
        );

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
