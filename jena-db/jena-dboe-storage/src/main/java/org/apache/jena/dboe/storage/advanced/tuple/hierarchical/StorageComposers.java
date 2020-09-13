package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.atlas.lib.tuple.Tuple;
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


//    public static <C, L, D> Meta2Node<Tuple<C>, D, C> parentMap(Meta2Node<L, D, C> child, int... tupleIdxs) {
//
//    }


    /**
     * A collection of tuples; corresponds to a Multimap<T, Void>
     *
     * @param <T>
     * @param <C>
     * @param accessor
     * @return
     */
    public static <T, C> Meta2Node<T, Void, C> collection(TupleAccessor<T, C> accessor) {
        return null;
    }

    /**
     * A single tuple; corresponds to Map<T, Void>
     *
     * @param <T>
     * @param <C>
     * @param accessor
     * @return
     */
    public static <T, C> Meta2Node<T, Void, C> singleton(TupleAccessor<T, C> accessor) {
        return null;
    }

    public static <V, C> Meta2Node<C, V, C> mapKeyComponent(int tupleIdx, Meta2Node<?, V, C>  child) {
        return null;
    }

    public static <V, C> Meta2Node<Tuple<C>, V, C> mapKeyTuple(Meta2Node<?, V, C>  child, int... tupleIdxs) {
        return null;
    }


    // If we had methods for factory mtehods for different arity we might even do type-safe forking
    // e.g. Map<Node, ForkPair2<Map<Node, Quad>, Map<Node, Map<...>>>>
    public static <C> Meta2Node<Tuple<C>, ?, C> fork(List<Meta2Node<?, ?, C>>  children) {
        return null;
    }


}


