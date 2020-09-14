package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

public class Meta2NodeAlt<D, C>
    extends Meta2NodeBase<D, C, List<Object>>
    implements Meta2NodeCompound<D, C, List<Object>>
{
    protected List<? extends Meta2NodeCompound<D, C, ?>> children;

    public Meta2NodeAlt(
            TupleAccessor<D, C> tupleAccessor,
            List<? extends Meta2NodeCompound<D, C, ?>> children
            ) {
        super(new int[] {}, tupleAccessor);
        this.children = children;
    }

    @Override
    public List<? extends Meta2Node<D, C, ?>> getChildren() {
        return children;
    }

    @Override
    public <T> Stream<?> streamEntries(List<Object> store, T tupleLike,
            TupleAccessorCore<? super T, ? extends C> tupleAccessor) {

        List<Object> childStores = asList(store);

        Meta2NodeCompound<D, C, ?> pickedChild = children.get(0);
        Object pickedChildStore = childStores.get(0);

        // Delegate always to the first entry - we would need external information to do better
        return pickedChild.streamEntriesRaw(pickedChildStore, tupleLike, tupleAccessor);
    }

    @SuppressWarnings("unchecked")
    public List<Object> asList(Object store) {
        return (List<Object>)store;
    }

    /**
     * Return of a list with fresh stores of all children
     *
     */
    @Override
    public List<Object> newStore() {
        List<Object> result = new ArrayList<>();
        for (Meta2NodeCompound<D, C, ?> child : children) {
            result.add(child.newStore());
        }
        return result;
    }

    /**
     * Checks whether all child store entries in the list of alternatives are empty
     *
     * (Not to be confused with checking the list of alternatives itself for emptiness)
     */
    @Override
    public boolean isEmpty(List<Object> childStores) {
        boolean result = true;
        for (int i = 0; i < children.size(); ++i) {
            Meta2NodeCompound<D, C, ?> child = children.get(i);
            Object childStore = childStores.get(i);

            result = child.isEmptyRaw(childStore);
            if (!result) {
                break;
            }
        }

        return result;
    }

    @Override
    public boolean add(List<Object> childStores, D tupleLike) {
        boolean result = false;
        for (int i = 0; i < children.size(); ++i) {
            Meta2NodeCompound<D, C, ?> child = children.get(i);
            Object childStore = childStores.get(i);

            result = result || child.addRaw(childStore, tupleLike);
        }

        return result;
    }

    @Override
    public boolean remove(List<Object> childStores, D tupleLike) {
        boolean result = false;
        for (int i = 0; i < children.size(); ++i) {
            Meta2NodeCompound<D, C, ?> child = children.get(i);
            Object childStore = childStores.get(i);

            result = result || child.removeRaw(childStore, tupleLike);
        }

        return result;
    }

    @Override
    public String toString() {
        return "<" + children.stream().map(Object::toString).collect(Collectors.joining(" | ")) + ">";
    }
}
