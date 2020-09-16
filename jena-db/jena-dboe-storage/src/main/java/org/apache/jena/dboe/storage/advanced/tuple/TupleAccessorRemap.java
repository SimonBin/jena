package org.apache.jena.dboe.storage.advanced.tuple;

/**
 * A forwarding tuple accessor that can remap indices
 * to shuffle the components
 *
 * @author raven
 *
 */
public class TupleAccessorRemap<TupleType, ComponentType>
    implements TupleAccessor<TupleType, ComponentType>
{
    protected TupleAccessor<TupleType, ComponentType> delegate;
    protected int[] remap;

    public TupleAccessorRemap(TupleAccessor<TupleType, ComponentType> delegate, int[] remap) {
        super();
        this.delegate = delegate;
        this.remap = remap;
    }

    @Override
    public int getRank() {
        return remap.length;
    }

    @Override
    public ComponentType get(TupleType domainObject, int idx) {
        int remappedIdx = remap[idx];
        ComponentType result = delegate.get(domainObject, remappedIdx);
        return result;
    }

    @Override
    public <T> TupleType restore(T obj, TupleAccessorCore<? super T, ? extends ComponentType> accessor) {
        //return delegate.restore(obj, accessor);
        throw new RuntimeException("implement me");

//        if (this.getRank() != delegate.getRank()) {
//            throw new IllegalStateException("Cannot delegate restoration of a domain object from a remapped tuple if the ranks differ");
//        }

//        validateRestoreArg(components);

//        int l = accessor.getRank();
//        @SuppressWarnings("unchecked")
//        ComponentType[] remppadComponents = (ComponentType[])new Object[l];
//
//        for (int i = 0; i < l; ++i) {
//            remppadComponents[remap[i]] = components[i];
//        }
//
//        TupleType result = delegate.restore(remppadComponents);
//        return result;
    }
}
