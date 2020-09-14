package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Meta2Node;
import org.apache.jena.ext.com.google.common.collect.Maps;

import com.github.andrewoma.dexx.collection.LinkedLists;
import com.github.andrewoma.dexx.collection.List;

public class Meta2NodeLib {

    /**
     * Helper method to find the shortest path / least nested index node without children
     * starting from a given one
     *
     *
     * @param <D>
     * @param <C>
     * @param node
     * @return
     */
    public static <D, C> java.util.List<Integer> findLeastNestedIndexNode(Meta2Node<D, C, ?> node) {

        java.util.List<Integer> result = new ArrayList<>();

        // Perform a depth first traversal of the sequence of index nodes
        // to find the shortest sequence which has no childrenO
        Stream<List<Entry<Meta2Node<D, C, ?>, Integer>>>
        breadthFirstPaths = BreadthFirstSearchLib.breadthFirstIndexedPaths(
                LinkedLists.of(Maps.immutableEntry(node, 0)), x -> x.getChildren().stream());

        Iterator<List<Entry<Meta2Node<D, C, ?>, Integer>>> it = breadthFirstPaths.iterator();
        while (it.hasNext()) {
            List<Entry<Meta2Node<D, C, ?>, Integer>> list = it.next();

            if (list.last().getKey().getChildren().isEmpty()) {
                // Found the path along index nodes with the least nesting

                list.forEach(entry -> { result.add(entry.getValue()); });
                break;
            }
        }

        return result;
    }
}
