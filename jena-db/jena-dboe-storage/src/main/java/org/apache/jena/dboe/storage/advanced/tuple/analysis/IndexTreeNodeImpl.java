package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.function.Function;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;

interface ForwardingFunction {

}


@FunctionalInterface
interface ComponentCallback<ComponentType> {

    /**
     * Receive the information that a component at some index has a certain value
     *
     * @param i
     * @param value
     */
    void receiveValue(int idx, ComponentType value);
}

public class IndexTreeNodeImpl<N, Self extends IndexTreeNodeImpl<N, Self>>
    implements IndexTreeNode<N, Self>
{
    @Override
    public IndexTreeNodeImpl<N, Self> getParent() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public N getValue() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Receive a key from
     *
     * @param key
     * @param forwardingFunction
     */
    public <TupleLike, ComponentType> void receiveKeyTuple(
            Object rootStore,
            // TODO
            TupleLike pattern,
            TupleAccessorCore<TupleLike, ComponentType> accessor,
            ComponentCallback<ComponentType> callback
    ) {
        // Stream tuples from the parent or an empty tuple if there is none
        //getParent().receiveKeyTuple(pattern, accessor, callback);

        if(getParent() != null) {
            //getParent()
        }

    }


}
