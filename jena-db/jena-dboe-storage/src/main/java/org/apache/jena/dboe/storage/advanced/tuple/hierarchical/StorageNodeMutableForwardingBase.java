package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

public class StorageNodeMutableForwardingBase<D, C, V, X extends StorageNodeMutable<D,C,V>>
    extends StorageNodeMutableForwarding<D, C, V, X>
{
    protected X delegate;

    public StorageNodeMutableForwardingBase(X delegate) {
        super();
        this.delegate = delegate;
    }

    @Override
    protected X getDelegate() {
        return delegate;
    }
}
