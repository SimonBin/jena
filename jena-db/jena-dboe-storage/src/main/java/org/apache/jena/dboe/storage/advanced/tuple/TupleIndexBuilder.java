package org.apache.jena.dboe.storage.advanced.tuple;

import org.apache.jena.atlas.lib.tuple.Tuple;

// Index vs Table: An index may not contain all information to reconstruct the domain objects
// An index may e.g. only keep track of counts for some combination of keys, however
// in this case the index would need to be notified by the storage whether actual deletion/insertion happen

/**
 * On the outside an index is a mapping of some tuple to a collection of  other ones - i.e a binary relation
 * The outside/frontend knows what values are actually mapped to in the end -e.g.
 * Domain Objects, Counts or even Zero-sized tuples (e.g. the collection of objects maps to nothing else)
 *
 * Internally there may be any number of collections or nested maps that eventually map to the frontend types
 *
 *
 *
 * @author raven
 *
 * @param <K>
 * @param <V>
 */
interface IndexFrontend<ComponentType, KeyType, V> {
    long weight(TupleQuery<ComponentType> tupleQuery);

}



// Key is a Tuple - special case of a Tuple1
interface IndexNodeComponent<K, V>
    extends IndexNodeTuple
{

}

// Key is a tuple
interface IndexNodeTuple {
    boolean canAsComponent();
    IndexNodeComponent asComponent();

}




interface TupleIndex<TupleType, DomainType, ComponentType> {
//    accept(Tuple tuple);

}

public class TupleIndexBuilder {
    public TupleIndexBuilder() {

    }

    TupleIndexBuilder withLinkedHashMapTo(int ... tupleIdxs) {
        return null;
    }

}
