package org.apache.jena.dboe.storage.advanced.tuple;

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
