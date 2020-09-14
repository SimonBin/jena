package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Util to wrap a structure with a starting item and a successor function
 * as 'tree nodes' - this adds links to the parents
 * and each node then corresponds to a specific path in the underlying structure
 *
 * @author raven
 *
 */
public class TreeLib {

    /**
     * Procedure to wrap each element in a tree-like (half order) structure expressed by means of a
     * starting node and a successor function.
     *
     * Performs post order depth first traversal
     *
     * Characteristics:
     * <ul>
     *   <li>Wrapped children are first collected in collection. Then this collection is passed to the ctor of the parent.</li>
     * </ul>
     *
     * @param <N> The node type for which to create wrapper
     * @param <W> The wrapped node type
     * @param <CC> The collection type in which to collect wrapped children
     * @param depth
     * @param node
     * @param successorFunction
     * @param newChildCollection
     * @param makeParent
     * @return
     */
    public static <N, W, CC extends Collection<W>> W createTreePostOrder(
            int depth,
            N node,
            Function<? super N, ? extends Iterable<? extends N>> successorFunction,
            Supplier<CC> newChildCollection,
            BiFunction<? super N, CC, ? extends W> makeParent
            ) {

        Iterable<? extends N> children = successorFunction.apply(node);
        CC wrappedChildren = newChildCollection.get();
        for (N child : children) {
            W wrappedChild = createTreePostOrder(depth + 1, child, successorFunction, newChildCollection, makeParent);
            wrappedChildren.add(wrappedChild);
        }

        // TODO Improve the design that the depth value is passed to construction
        W result = makeParent.apply(node, wrappedChildren);
        return result;
    }

    /**
     * Procedure to wrap each element in a tree-like (half order) structure expressed by means of a
     * starting node and a successor function.
     *
     * Performs pre order depth first traversal
     *
     * Characteristics:
     * <ul>
     *   <li>Because children are not known at construction time wrappers node must be mutable</li>
     *   <li>Depth values are known in advance - if the depth value is relevant then the
     *   newWrappedNode function can obtain it from the wrapped parent (and add 1 to it)</li>
     * </ul>
     *
     * @param <N>
     * @param <W>
     * @param <CC>
     * @param wrappedParent
     * @param node
     * @param successorFunction
     * @param newWrappedNode
     * @param appendChild
     * @return
     */
    public static <N, W, CC extends Collection<W>> W createTreePreOrder(
            W wrappedParent,
            N node,
            Function<? super N, ? extends Iterable<? extends N>> successorFunction,
            BiFunction<? super N, ? super W, ? extends W> newWrappedNode,
            BiConsumer<? super W, ? super W> appendChild
            ) {

        W result = newWrappedNode.apply(node, wrappedParent);

        Iterable<? extends N> children = successorFunction.apply(node);
        for (N child : children) {
            W wrappedChild = createTreePreOrder(result, child, successorFunction, newWrappedNode, appendChild);
            appendChild.accept(result, wrappedChild);
        }

        return result;
    }


}
