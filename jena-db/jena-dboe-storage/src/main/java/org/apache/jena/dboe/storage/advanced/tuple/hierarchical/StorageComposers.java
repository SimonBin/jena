package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.ext.com.google.common.base.Functions;

/**
 * The APIs for building the index from the root become so utterly complex;
 * let's try with a bottom up approach
 *
 * Update: This looks much better - even good - now
 *
 *
 * @author raven
 *
 */
public class StorageComposers {

    public static <D, C> StorageNodeMutable<D, C, Set<D>> leafSet(
            TupleAccessor<D, C> tupleAccessor,
            SetSupplier setSupplier) {
        return new StorageNodeLeafSet<D, C, D>(
                tupleAccessor,
                setSupplier,
                // Ugly identity mapping of domain tuples to themselves as values - can we do better?
                TupleValueFunction.newIdentity()
                );
    }

    public static <D, C> StorageNodeMutable<D, C, Map<C, D>> leafMap(
            int tupleIdx,
            TupleAccessor<D, C> tupleAccessor,
            MapSupplier mapSupplier) {
        return new StorageNodeLeafMap<D, C, C, D>(
                new int[] {tupleIdx},
                tupleAccessor,
                mapSupplier,
                TupleValueFunction::component0,
                (key, idx) -> key, // TODO Ensure that only component 0 is requested
                // Ugly identity mapping of domain tuples to themselves as values - can we do better?
                TupleValueFunction.newIdentity()
                );
    }


    /**
     * TODO Think this through
     *
     * Similar to innerMap - however the values are custom intermediary object
     * that will hold a further index nodes.
     *
     * <pre>
     * forwardingInnerMap(
     *   mapSupplier for Map<K, TripleTableCore>,
     *   TupleTableCore::getIndexRoot
     * )
     * </pre>
     *
     * @param <D>
     * @param <C>
     * @param <V>
     * @param tupleIdx
     * @param mapSupplier
     * @param child
     * @return
     */
    public static <D, C, V> StorageNodeMutable<D, C, Map<C, V>> forwardingInnerMap(
            int tupleIdx,
            MapSupplier mapSupplier,
            StorageNodeMutable<D, C, V> child
            ) {

        return null;
    }


    public static <D, C, V> StorageNodeMutable<D, C, Map<C, V>> innerMap(
            int tupleIdx,
            MapSupplier mapSupplier,
            StorageNodeMutable<D, C, V> child
            ) {

        TupleAccessor<D, C> tupleAccessor = child.getTupleAccessor();

        return new StorageNodeInnerMap<D, C, C, V>(
                new int[] {tupleIdx},
                tupleAccessor,
                child,
                mapSupplier,
                TupleValueFunction::component0,
                (key, idx) -> key // TODO Ensure that only component 0 is requested
                );
                // Return the element at index 0 of any tuple like object
                // (tupleLike, tupleAccessor) -> tupleAccessor.get(tupleLike, 0));
    }


    /**
     * Generic construction for composition from multiple composers
     * Breaks strong typing in contrast to static alternatives{1, 2, ...} constructions that
     * could e.g. yield Map<Foo, Alternatives3<Bar, Baz, Bax>> types
     *
     * @param <D>
     * @param <C>
     * @param <V>
     * @param tupleIdx
     * @param mapSupplier
     * @param child
     * @return
     */
    public static <D, C> StorageNodeMutable<D, C, ?> altN(
            List<? extends StorageNodeMutable<D, C, ?>> children
            ) {

        if (children.isEmpty()) {
            throw new IllegalArgumentException("At least one alternative must be provided");
        }

        // TODO Validate that all children use the same tuple acessor
        TupleAccessor<D, C> tupleAccessor = children.get(0).getTupleAccessor();
        return new StorageNodeAltN<D, C>(tupleAccessor, children);
    }


    public static <D, C, V1, V2> StorageNodeMutable<D, C, Entry<V1, V2>> alt2(
            StorageNodeMutable<D, C, V1> child1,
            StorageNodeMutable<D, C, V2> child2
            ) {

        // TODO Validate that all children use the same tuple acessor
        TupleAccessor<D, C> tupleAccessor = child1.getTupleAccessor();
        return new StorageNodeAlt2<D, C, V1, V2>(tupleAccessor, child1, child2);
    }

}


