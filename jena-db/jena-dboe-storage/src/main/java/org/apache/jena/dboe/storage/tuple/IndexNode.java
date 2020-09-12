package org.apache.jena.dboe.storage.tuple;

import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;


/**
 * An index node is conceptually backed by a Multimap<K, V> (with V either another Map or a Collection.
 * (or rather Map<K, Collection<V>?)
 * The items in the key and value sets are conceptually Tuples of ComponentType.
 * Methods for using ComponentType directly instead of one-tuples are provided which may not physically create
 * tuple-one objects.
 *
 * An index node allows setting constraints on the keys and obtain statistics about
 * set of matching keys and values.
 *
 *
 * Furthermore if V is another Multimap then
 *
 *
 * @author raven
 *
 * @param <ComponentType>
 */
public interface IndexNode<ComponentType> {
    /**
     * The parent index node or null if there is none
     *
     * @return
     */
    IndexNodeFork<ComponentType> getParent();

    /**
     * Builder to create constraints on this index node which can then return a new IndexNode which
     * is this one plus the constraints
     *
     * The new IndexNode has the same parent as this one
     * So far there is no origin/provenance link of the created IndexNode back to this one
     *
     * @return
     */
    default ConstraintBuilder<ComponentType> contraints() {
        return new ConstraintBuilderBase<ComponentType>(this);
    }


    IndexNodeFork<ComponentType> forConstraints(TupleConstraint<ComponentType> constraints);

    /**
     * Number of components on this index node.
     * Constraints on this IndexeNode can only affect this many components
     *
     * @return
     */
    int getRank();

    Stream<Tuple<ComponentType>> streamEntries();
}

