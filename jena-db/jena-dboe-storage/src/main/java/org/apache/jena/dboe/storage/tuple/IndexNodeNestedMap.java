package org.apache.jena.dboe.storage.tuple;

import java.util.Map;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;

public class IndexNodeNestedMap<ComponentType, V, SubMapType extends Map<ComponentType, V>>
    extends IndexNodeBase<ComponentType>
{

    protected Map<ComponentType, SubMapType> map;
    protected SubIndexNodeFactory<ComponentType, SubMapType> subIndexNodeFactory;

    /**
     * An index node based on a map.
     * A factory function yields a child index node for a map entry
     *
     *
     * @param map
     */
    public IndexNodeNestedMap(
            IndexNodeFork<ComponentType> parent,
            Map<ComponentType, SubMapType> map,
            SubIndexNodeFactory<ComponentType, SubMapType> subIndexNodeFactory) {
        super(parent);
        this.map = map;
        this.subIndexNodeFactory = subIndexNodeFactory;
    }

//    @Override
//    public Map<Integer, ? extends IndexNode<ComponentType>> getComponentToSubIndex(ComponentType constraint) {
//        SubMapType value = map.get(constraint);
//        Map<Integer, ? extends IndexNode<ComponentType>> result = subIndexNodeFactory.create(this, value);
//        return result;
//    }

//    @Override
//    public long estimateRemainingValueCount(ComponentType constraint) {
//        SubMapType value = map.get(constraint);
//        int result = value == null ? 0 : map.size();
//        return result;
//    }


    public static <ComponentType, V, SubMapType extends Map<ComponentType, V>> IndexNode<ComponentType> create(
            IndexNodeFork<ComponentType> parent,
            Map<ComponentType, SubMapType> map,
            SubIndexNodeFactory<ComponentType, SubMapType> subIndexNodeFactory) {
        return new IndexNodeNestedMap<>(parent, map, subIndexNodeFactory);
    }

    @Override
    public IndexNodeFork<ComponentType> forConstraints(TupleConstraint<ComponentType> constraints) {

        // TODO validate
        ComponentType key = constraints.getConstraints().get(0);
        SubMapType value = map.get(key);

        IndexNodeFork<ComponentType> result = subIndexNodeFactory.create(this, value);
        return result;
    }

    @Override
    public int getRank() {
        return 1;
    }

    @Override
    public Stream<Tuple<ComponentType>> streamEntries() {
        return null;

    }

}
