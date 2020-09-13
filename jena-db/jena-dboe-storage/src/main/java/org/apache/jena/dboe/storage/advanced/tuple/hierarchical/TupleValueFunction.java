package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

/**
 * Map a tuple-like object to a value
 *
 * @author raven
 *
 */
public interface TupleValueFunction<ComponentType, ValueType> {
    <TupleLike> ValueType map(TupleLike tupleLike, TupleAccessorCore<? super TupleLike, ? extends ComponentType> tupleAccessor);


    /**
     * TupleValueFunction that returns the tuple itself as the value
     *
     * @param <T>
     * @param <C>
     * @param tupleLike
     * @param tupleAccessor
     * @return
     */
    public static <ComponentType, ValueType> TupleValueFunction<ComponentType, ValueType> newIdentity() {
        return new TupleValueFunction<ComponentType, ValueType>() {
            @Override
            public <TupleLike> ValueType map(TupleLike tupleLike, TupleAccessorCore<? super TupleLike, ? extends ComponentType> tupleAccessor) {
              return (ValueType)tupleLike;
            }
        };
    }

//    public static <T, C> T identity(T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor) {
//        return tupleLike;
//    }

    /**
     * TupleValueFunction-compatible method that returns the component at index 0
     *
     * @param <T>
     * @param <C>
     * @param tupleLike
     * @param tupleAccessor
     * @return
     */
    public static <T, C> C component0(T tupleLike, TupleAccessorCore<? super T, ? extends C> tupleAccessor) {
        return tupleAccessor.get(tupleLike, 0);
    }

}
