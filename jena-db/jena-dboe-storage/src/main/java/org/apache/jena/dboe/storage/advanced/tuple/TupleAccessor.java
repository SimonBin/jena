package org.apache.jena.dboe.storage.advanced.tuple;

/**
 * Access components of a domain object such as a Triple or Quad as if it was a Tuple
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <DomainType>
 */
public interface TupleAccessor<DomainType, ComponentType>
    extends TupleAccessorCore<DomainType, ComponentType>
{
    int getRank();

    /**
     * Restore a domain object from some other object with a corresponding accessor
     * The length of the component array must equal the rank of the accessor
     *
     * @param <T> The type of tuple-like domain object from which to copy its components
     * @param obj The domain object
     * @param accessor The accessor of components from the domain object
     * @return
     */
    <T> DomainType restore(T obj, TupleAccessor<? super T, ? extends ComponentType> accessor);

    default void validateRestoreArg(TupleAccessor<?, ?> accessor) {
        int cl = accessor.getRank();
        int r = getRank();

        if (cl != r) {
            throw new IllegalArgumentException("components.length must equal rank but " + cl + " != " + r);
        }

    }

    default ComponentType[] toComponentArray(DomainType domainObject) {
        int rank = getRank();
        @SuppressWarnings("unchecked")
        ComponentType[] result = (ComponentType[])new Object[rank];

        for (int i = 0; i < rank; ++i) {
            result[i] = get(domainObject, i);
        }

        return result;
    }

}
