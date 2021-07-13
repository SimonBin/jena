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

package org.apache.jena.dboe.storage.advanced.tuple.api;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <DomainType>
 * @param <ComponentType>
 * @param <TableType>
 */
public abstract class TupleTableCore2<DomainType, ComponentType, TableType extends TupleTableCore<DomainType, ComponentType>>
    implements TupleTableCore<DomainType, ComponentType>
{
    protected TableType primary;
    protected TableType secondary;

    public TupleTableCore2(TableType primary, TableType secondary) {
        super();
        this.primary = primary;
        this.secondary = secondary;
    }

    @Override
    public void clear() {
        primary.clear();
        secondary.clear();
    }

    @Override
    public void add(DomainType quad) {
        primary.add(quad);
        secondary.add(quad);
    }

    @Override
    public void delete(DomainType quad) {
        primary.delete(quad);
        secondary.delete(quad);
    }

    @Override
    public boolean contains(DomainType tuple) {
        return primary.contains(tuple);
    }

    @Override
    public long size() {
        return primary.size();
    }

    @Override
    public int getDimension() {
        return primary.getDimension();
    }

//    @Override
//    public <T> Stream<DomainType> find(T lookup, TupleAccessor<? super T, ? extends ComponentType> accessor) {
//        // TODO Auto-generated method stub
//        return null;
//    }
}
