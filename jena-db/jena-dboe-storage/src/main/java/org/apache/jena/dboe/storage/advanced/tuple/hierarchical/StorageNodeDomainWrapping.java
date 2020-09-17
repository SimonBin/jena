package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.function.BiFunction;

public class StorageNodeDomainWrapping<D, C, V, X extends StorageNodeMutable<D, C, V>>
    extends StorageNodeMutableForwarding<D, C, V, X>
{
    protected X target;
    protected BiFunction<? super StorageNodeMutable<D, C, V>, ? super X, ? extends X> storeWrapper;

    public StorageNodeDomainWrapping(X target,
            BiFunction<? super StorageNodeMutable<D, C, V>, ? super X, ? extends X> storeWrapper) {
        super();
        this.target = target;
        this.storeWrapper = storeWrapper;
    }

    @Override
    protected X getDelegate() {
        return target;
    }

    @Override
    public V newStore() {
//        V store = target.newStore();
//        V result = storeWrapper.apply(this, store);
//        return result;
        return null;
    }
}
