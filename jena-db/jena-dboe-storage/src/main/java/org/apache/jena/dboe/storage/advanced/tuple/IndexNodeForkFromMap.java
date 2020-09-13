package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.jena.ext.com.google.common.collect.Multimap;
import org.apache.jena.ext.com.google.common.collect.Multimaps;

public class IndexNodeForkFromMap<ComponentType>
    extends IndexNodeForkBase<ComponentType>
{
    protected Multimap<List<Integer>, IndexNodeFactory<ComponentType>> choices;

    public IndexNodeForkFromMap(
            IndexNode<ComponentType> parent,
            Multimap<List<Integer>, IndexNodeFactory<ComponentType>> choices) {
        super(parent);
        this.choices = choices;
    }

    @Override
    public Multimap<List<Integer>, IndexNodeFactory<ComponentType>> choices() {
        return choices;
    }

//    @Override
//    public IndexNode<ComponentType> descend(List<Integer> option) {
//        Function<IndexNodeFork<ComponentType>, IndexNode<ComponentType>> factory = choices.get(option);
//
//        if (factory == null) {
//            throw new IllegalArgumentException("Option " + option + " has no entry within " + choices.keySet());
//        }
//
//        IndexNode<ComponentType> result = factory.apply(this);
//        return result;
//    }

    public static <ComponentType> IndexNodeFork<ComponentType> create(
            IndexNode<ComponentType> parent,
            Multimap<List<Integer>, IndexNodeFactory<ComponentType>> choices) {
        return new IndexNodeForkFromMap<>(parent, choices);
    }

    public static <ComponentType> IndexNodeFork<ComponentType> singleton(
            IndexNode<ComponentType> parent,
            Integer index,
            IndexNodeFactory<ComponentType> factory) {
        return singleton(parent, Arrays.asList(index), factory);
    }

    public static <ComponentType> IndexNodeFork<ComponentType> empty(
            IndexNode<ComponentType> parent) {
        return new IndexNodeForkFromMap<>(parent, Multimaps.forMap(Collections.emptyMap()));
    }

    public static <ComponentType> IndexNodeFork<ComponentType> singleton(
            IndexNode<ComponentType> parent,
            List<Integer> indexes,
            IndexNodeFactory<ComponentType> factory) {
        return new IndexNodeForkFromMap<>(parent, Multimaps.forMap(Collections.singletonMap(indexes, factory)));
    }

    @Override
    public long estimateIndexSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long estimateRemainingValueCount(ComponentType constraint) {
        // TODO Auto-generated method stub
        return 0;
    }

//
//    default Stream<Tuple<ComponentType>> streamEntriesMap(Tuple<ComponentType> parentKey, Map<V, Collection<C>> subMap) {
//
//    }
//
//    default Stream<Tuple<ComponentType>> streamEntriesMap() {
//        getParent()
//    }
}
