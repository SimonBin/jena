package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;

public class TupleAccessorTuple<ComponentType>
    implements TupleAccessor<Tuple<ComponentType>, ComponentType>
{
    protected int rank;

    public TupleAccessorTuple(int rank) {
        super();
        this.rank = rank;
    }

    @Override
    public int getDimension() {
        return rank;
    }

    @Override
    public ComponentType get(Tuple<ComponentType> domainObject, int idx) {
        return domainObject.get(idx);
    }

    @Override
    public <T> Tuple<ComponentType> restore(T obj, TupleAccessorCore<? super T, ? extends ComponentType> accessor) {
//        validateRestoreArg(accessor);

        List<ComponentType> xs = new ArrayList<>(rank);
        for (int i = 0; i < rank; ++i) {
            xs.set(i, accessor.get(obj, i));
        }
        return TupleFactory.create(xs);
//        ComponentType[] xs = accessor.toComponentArray(obj);
    }

}
