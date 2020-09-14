package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.ext.com.google.common.collect.Maps;

public class Meta2NodeAltN<D, C>
    extends Meta2NodeNoKeyBase<D, C, Object[]>
    implements Meta2NodeCompound<D, C, Object[]>
{
    protected List<? extends Meta2NodeCompound<D, C, ?>> children;

    public Meta2NodeAltN(
            TupleAccessor<D, C> tupleAccessor,
            List<? extends Meta2NodeCompound<D, C, ?>> children
            ) {
        super(tupleAccessor);
        this.children = children;
    }

    @Override
    public List<? extends Meta2Node<D, C, ?>> getChildren() {
        return children;
    }

    @Override
    public <T> Stream<?> streamEntries(Object[] childStores, T tupleLike,
            TupleAccessorCore<? super T, ? extends C> tupleAccessor) {

        Meta2NodeCompound<D, C, ?> pickedChild = children.get(0);
        Object pickedChildStore = childStores[0];

        // Delegate always to the first entry - we would need external information to do better
        return pickedChild.streamEntriesRaw(pickedChildStore, tupleLike, tupleAccessor);
    }

    /**
     * Return of a list with fresh stores of all children
     *
     */
    @Override
    public Object[] newStore() {
        Object[] result = new Object[children.size()];
        for (int i = 0; i < result.length; ++i) {
            result[i] = children.get(i).newStore();
        }
        return result;
    }

    /**
     * Checks whether all child store entries in the list of alternatives are empty
     *
     * (Not to be confused with checking the list of alternatives itself for emptiness)
     */
    @Override
    public boolean isEmpty(Object[] childStores) {
        Meta2NodeCompound<D, C, ?> pickedChild = children.get(0);
        Object pickedChildStore = childStores[0];

        boolean result = pickedChild.isEmptyRaw(pickedChildStore);
        return result;
    }

    @Override
    public boolean add(Object[] childStores, D tupleLike) {
        boolean result = false;
        for (int i = 0; i < children.size(); ++i) {
            Meta2NodeCompound<D, C, ?> child = children.get(i);
            Object childStore = childStores[i];

            result = result || child.addRaw(childStore, tupleLike);
        }

        return result;
    }

    @Override
    public boolean remove(Object[] childStores, D tupleLike) {
        boolean result = false;
        for (int i = 0; i < children.size(); ++i) {
            Meta2NodeCompound<D, C, ?> child = children.get(i);
            Object childStore = childStores[i];

            result = result || child.removeRaw(childStore, tupleLike);
        }

        return result;
    }

    @Override
    public String toString() {
        return "<" + children.stream().map(Object::toString).collect(Collectors.joining(" | ")) + ">";
    }


//    @Override
//    public <T> Streamer<Object[], ? extends Entry<?, ?>> streamerForKeyAndSubStores(
//            int altIdx,
//            T pattern,
//            TupleAccessorCore<? super T, ? extends C> accessor) {
//        return argStore -> Stream.of(Maps.immutableEntry(TupleFactory.create0(), chooseSubStore(argStore, altIdx)));
//    }

    @Override
    public Object chooseSubStore(Object[] store, int subStoreIdx) {
        return store[subStoreIdx];
    }
}
