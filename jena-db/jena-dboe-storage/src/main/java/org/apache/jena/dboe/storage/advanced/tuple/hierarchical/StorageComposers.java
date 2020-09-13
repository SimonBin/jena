package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;

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

    public static <D, C> Meta2NodeCompound<D, C, Set<D>> leafSet(
            TupleAccessor<D, C> tupleAccessor,
            SetSupplier setSupplier) {
        return new Meta2NodeLeafSet<D, C, D>(
                new int[] {}, // Indexed by no components
                tupleAccessor,
                setSupplier,
                // Ugly identity mapping of domain tuples to themselves as values - can we do better?
                TupleValueFunction.newIdentity()
                );
    }

    public static <D, C> Meta2NodeCompound<D, C, Map<C, D>> leafMap(
            int tupleIdx,
            TupleAccessor<D, C> tupleAccessor,
            MapSupplier mapSupplier) {
        return new Meta2NodeLeafMap<D, C, C, D>(
                new int[] {tupleIdx},
                tupleAccessor,
                mapSupplier,
                TupleValueFunction::component0,
                // Ugly identity mapping of domain tuples to themselves as values - can we do better?
                TupleValueFunction.newIdentity()
                );
    }


    public static <D, C, V> Meta2NodeCompound<D, C, Map<C, V>> innerMap(
            int tupleIdx,
            MapSupplier mapSupplier,
            Meta2NodeCompound<D, C, V> child
            ) {

        TupleAccessor<D, C> tupleAccessor = child.getTupleAccessor();

        return new Meta2NodeInnerMap<D, C, C, V>(
                new int[] {tupleIdx},
                tupleAccessor,
                child,
                mapSupplier,
                TupleValueFunction::component0
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
    public static <D, C> Meta2NodeCompound<D, C, ?> altN(
            List<? extends Meta2NodeCompound<D, C, ?>> children
            ) {

        if (children.isEmpty()) {
            throw new IllegalArgumentException("At least one alternative must be provided");
        }

        // TODO Validate that all children use the same tuple acessor
        TupleAccessor<D, C> tupleAccessor = children.get(0).getTupleAccessor();
        return new Meta2NodeAlt<D, C>(tupleAccessor, children);
    }

}


