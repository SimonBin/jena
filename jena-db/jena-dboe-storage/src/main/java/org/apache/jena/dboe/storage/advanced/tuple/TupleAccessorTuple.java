package org.apache.jena.dboe.storage.advanced.tuple;

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
    public int getRank() {
        return rank;
    }

    @Override
    public ComponentType get(Tuple<ComponentType> domainObject, int idx) {
        return domainObject.get(idx);
    }

    @Override
    public <T> Tuple<ComponentType> restore(T obj, TupleAccessor<? super T, ? extends ComponentType> accessor) {
        validateRestoreArg(accessor);

        ComponentType[] xs = accessor.toComponentArray(obj);
        Tuple<ComponentType> result = TupleFactory.create(xs);
        return result;
    }

}
