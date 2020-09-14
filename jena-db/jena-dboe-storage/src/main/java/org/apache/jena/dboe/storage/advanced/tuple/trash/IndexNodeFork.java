package org.apache.jena.dboe.storage.advanced.tuple.trash;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleConstraint;
import org.apache.jena.ext.com.google.common.collect.Multimap;

import com.github.jsonldjava.shaded.com.google.common.collect.Iterables;


/**
 *
 *
 *
 * @author raven
 *
 * @param <ComponentType>
 */
public interface IndexNodeFork<ComponentType> {

    /**
     * Get the constraints that apply to this fork's IndexNode
     * Does not include those of the ancestors
     *
     * @return
     */
    TupleConstraint<ComponentType> getConststraintContribution();

    /**
     * Get the constraints of this and all ancestor nodes
     * Does not include those of any children
     *
     * @return
     */
    TupleConstraint<ComponentType> getOveralConstraints();


    IndexNode<ComponentType> getParent();

    Multimap<List<Integer>, IndexNodeFactory<ComponentType>> choices();

    default Collection<IndexNodeFactory<ComponentType>> choose(int idx) {
        Collection<IndexNodeFactory<ComponentType>> factories = choices().get(Arrays.asList(idx));
        return factories;
    }

    default IndexNode<ComponentType> chooseFirst(int idx) {
        IndexNodeFactory<ComponentType> factory = Iterables.getFirst(choose(idx), null);
        return factory.create(this);
    }

    /**
     * Returns list of arrays of component indexes of the sub indexes
     *
     * [ [3], [1, 2], [3] ] would denote 3 indices such as for G, PO and G again.
     * Typcially the 2 Gs should fork into different sub indexes
     *
     * @return
     */
//    Collection<List<Integer>> listSubIndexes();

    /**
     *
     * @param optionIdx the *index* of an item returned by listSubIndexes()
     * Multiple indexes may use the same tuple indexes
     *
     * @return
     */
//    IndexNode<ComponentType> descend(int optionIdx);

//    default IndexNode<ComponentType> descend(int componentIdx) {
//        return descend(Arrays.asList(componentIdx));
//    }

    //Map<List<Integer>, IndexNode<ComponentType>> getChoices();

    public static <ComponentType> IndexNodeFork<ComponentType> empty(IndexNode<ComponentType> parent) {
        return IndexNodeForkFromMap.empty(parent);
    }

    long estimateIndexSize();
    long estimateRemainingValueCount(ComponentType constraint);


    /**
     * Streams all keys that match the given constraints and for each key
     * yields a consumer that sets components of a tuple such that they match the key
     *
     * @return
     */
//    Stream<Consumer<List<ComponentType>>> streamContributors();
}
