package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorCore;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Meta2Node;


public class IndexTreeNodeImpl<D, C>
//    implements IndexTreeNode
{
    protected Meta2Node<D, C, ?> storage;
    protected IndexTreeNodeImpl<D, C> parent;
    protected List<IndexTreeNodeImpl<D, C>> children = new ArrayList<>();

    public IndexTreeNodeImpl(
            Meta2Node<D, C, ?> storage,
            IndexTreeNodeImpl<D, C> parent) {
        super();
        this.storage = storage;
        this.parent = parent;
    }

    public static <D, C> IndexTreeNodeImpl<D, C> bakeTree(Meta2Node<D, C, ?> root) {

        IndexTreeNodeImpl<D, C> result = TreeLib.<Meta2Node<D, C, ?>, IndexTreeNodeImpl<D, C>>createTreePreOrder(
                null,
                root,
                Meta2Node::getChildren,
                IndexTreeNodeImpl::new,
                IndexTreeNodeImpl::addChild);

        return result;
    }


    public void addChild(IndexTreeNodeImpl<D, C> child) {
        children.add(child);
    }



    /**
     * Receive a key from
     *
     * @param key
     * @param forwardingFunction
     */
    public <TupleLike, ComponentType> void receiveKeyTuple(
            // TODO
            TupleLike pattern,
            TupleAccessorCore<TupleLike, ComponentType> accessor
    ) {
        // Create the streamers for all parents

//        IndexTreeNodeImpl<N, Self> start = this;





        // Stream tuples from the parent or an empty tuple if there is none
        //getParent().receiveKeyTuple(pattern, accessor, callback);

//        if(getParent() != null) {
//            //getParent()
//        }

    }


}
