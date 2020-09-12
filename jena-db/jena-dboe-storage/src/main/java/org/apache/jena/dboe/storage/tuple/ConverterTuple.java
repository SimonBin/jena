package org.apache.jena.dboe.storage.tuple;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.ext.com.google.common.base.Converter;

/**
 * A converter from any domain object type to Tuple<ComponentType> via a TupleAccessor
 *
 * @author raven
 *
 * @param <DomainType>
 * @param <ComponentType>
 */
public class ConverterTuple<DomainType, ComponentType>
    extends Converter<DomainType, Tuple<ComponentType>>
{
    /**
     * Accessor for accessing the components of jena tuples; requires initialization with a rank
     */
    protected TupleAccessor<Tuple<ComponentType>, ComponentType> identityAccessor;


    /**
     * Accessor for the domain type such as for extracting Nodes from Triples or Quads
     *
     */
    protected TupleAccessor<DomainType, ComponentType> domainAccessor;


    public ConverterTuple(TupleAccessor<DomainType, ComponentType> tupleAccessor) {
        super();
        this.domainAccessor = tupleAccessor;
        this.identityAccessor = new TupleAccessorTuple<>(tupleAccessor.getRank());
    }

    @Override
    protected DomainType doBackward(Tuple<ComponentType> arg) {
        DomainType result = domainAccessor.restore(arg, identityAccessor);
        return result;
    }

    @Override
    protected Tuple<ComponentType> doForward(DomainType arg) {
        Tuple<ComponentType> result = TupleFactory.create(domainAccessor.toComponentArray(arg));
        return result;
    }

}
