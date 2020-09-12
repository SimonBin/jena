package org.apache.jena.dboe.storage.tuple;

import java.util.Collection;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;


public class IndexNodeCollection<ComponentType>
    extends IndexNodeBase<ComponentType>
{
    protected Collection<? extends ComponentType> collection;

    /**
     * An index node based on a map.
     * A factory function yields a child index node for a map entry
     *
     *
     * @param map
     */
    public IndexNodeCollection(
            IndexNodeFork<ComponentType> parent,
            Collection<? extends ComponentType> collection) {
        super(parent);
        this.collection = collection;
    }

//    @Override
//    public Map<Integer, ? extends IndexNode<ComponentType>> getComponentToSubIndex(ComponentType constraint) {
//        return Collections.emptyMap();
//    }

//    @Override
//    public long estimateRemainingValueCount(ComponentType constraint) {
//        return 1;
//    }

    public static <ComponentType> IndexNode<ComponentType> create(
            IndexNodeFork<ComponentType> parent,
            Collection<? extends ComponentType> collection) {
        return new IndexNodeCollection<>(parent, collection);
    }

    @Override
    public IndexNodeFork<ComponentType> forConstraints(TupleConstraint<ComponentType> constraints) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getRank() {
        return 1;
    }

    @Override
    public Stream<Tuple<ComponentType>> streamEntries() {
        // TODO Auto-generated method stub
        return null;
    }

}
