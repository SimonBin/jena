package org.apache.jena.dboe.storage.advanced.tuple;

/**
 * getter and setter in one
 *
 * @author raven
 *
 */
public interface TupleMutator<DomainType, ComponentType>
    extends TupleAccessor<DomainType, ComponentType>, TupleSetter<DomainType, ComponentType>
{
}
