package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

import com.github.jsonldjava.shaded.com.google.common.collect.Maps;

public class Meta2NodeAlt2<D, C, V1, V2>
    extends Meta2NodeBase<D, C, Entry<V1, V2>>
    implements Meta2NodeCompound<D, C, Entry<V1, V2>>
{
    // protected List<? extends Meta2NodeCompound<D, C, ?>> children;
    protected Entry<? extends Meta2NodeCompound<D, C, V1>, ? extends Meta2NodeCompound<D, C, V2>> children;

    public Meta2NodeAlt2(
            TupleAccessor<D, C> tupleAccessor,
            Meta2NodeCompound<D, C, V1> child1,
            Meta2NodeCompound<D, C, V2> child2
        ) {
        super(new int[] {}, tupleAccessor);
        this.children = Maps.immutableEntry(child1, child2);
    }

    @Override
    public List<? extends Meta2Node<D, C, ?>> getChildren() {
        return Arrays.asList(children.getKey(), children.getValue());
    }

    @Override
    public <T> Stream<?> streamEntries(Entry<V1, V2> childStores, T tupleLike,
            TupleAccessorCore<? super T, ? extends C> tupleAccessor) {

        Meta2NodeCompound<D, C, ?> pickedChild = children.getKey();
        Object pickedChildStore = childStores.getKey();

        // Delegate always to the first entry - we would need external information to do better
        return pickedChild.streamEntriesRaw(pickedChildStore, tupleLike, tupleAccessor);
    }

    /**
     * Return of a list with fresh stores of all children
     *
     */
    @Override
    public Entry<V1, V2> newStore() {
        return Maps.immutableEntry(children.getKey().newStore(), children.getValue().newStore());
    }

    /**
     * Checks whether all child store entries in the list of alternatives are empty
     *
     * (Not to be confused with checking the list of alternatives itself for emptiness)
     */
    @Override
    public boolean isEmpty(Entry<V1, V2> childStores) {
        Meta2NodeCompound<D, C, ?> pickedChild = children.getKey();
        Object pickedChildStore = childStores.getKey();

        boolean result = pickedChild.isEmptyRaw(pickedChildStore);
        return result;
    }

    @Override
    public boolean add(Entry<V1, V2> childStores, D tupleLike) {
        boolean result = false;
        result = result || children.getKey().add(childStores.getKey(), tupleLike);
        children.getValue().add(childStores.getValue(), tupleLike);

        return result;
    }

    @Override
    public boolean remove(Entry<V1, V2> childStores, D tupleLike) {
        boolean result = children.getKey().remove(childStores.getKey(), tupleLike);
        children.getValue().remove(childStores.getValue(), tupleLike);

        return result;
    }

    @Override
    public String toString() {
        return "<" + getChildren().stream().map(Object::toString).collect(Collectors.joining(" | ")) + ">";
    }
}