package org.apache.jena.dboe.storage.tuple;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public abstract class TupleTableBase<TupleType, ComponentType>
    implements TupleTableCore<TupleType, ComponentType>
{
    @Override
    public final Stream<TupleType> findTuples(List<ComponentType> pattern) {
//        assertEqual(getRank(), pattern.length, "Pattern length must be equal to columns in tuple table");
        return checkedFindTuples(pattern);
    }

    @SuppressWarnings("unchecked")
    protected abstract Stream<TupleType> checkedFindTuples(List<ComponentType> pattern);


    public static void assertEqual(Object a, Object b, String message) {
        if (!Objects.equals(a, b)) {
            throw new IllegalArgumentException(message + "; " + a + " != " + b);
        }
    }
}
