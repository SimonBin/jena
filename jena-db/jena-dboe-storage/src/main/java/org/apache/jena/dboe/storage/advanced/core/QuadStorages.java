package org.apache.jena.dboe.storage.advanced.core;

import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.alt2;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.innerMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafMap;
import static org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageComposers.leafSet;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import org.apache.jena.dboe.storage.advanced.tuple.api.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNodeMutable;
import org.apache.jena.graph.Node;

public class QuadStorages {
    public static <T> StorageNodeMutable<T, Node, ?> createQuadStorage(
            boolean strictOrder,
            TupleAccessor<T, Node> tupleAccessors) {

        StorageNodeMutable<T, Node, ?> result =
            innerMap(3, LinkedHashMap::new,
                innerMap(0, LinkedHashMap::new,
                    innerMap(1, LinkedHashMap::new,
                        leafMap(2, LinkedHashMap::new, tupleAccessors))));

        if (strictOrder) {
            result = alt2(result, leafSet(LinkedHashSet::new, tupleAccessors));
        }

        return result;
    }
}
