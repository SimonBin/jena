/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */
package org.apache.jena.dboe.storage.advanced.tuple;

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
        this.identityAccessor = new TupleAccessorTuple<>(tupleAccessor.getDimension());
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
