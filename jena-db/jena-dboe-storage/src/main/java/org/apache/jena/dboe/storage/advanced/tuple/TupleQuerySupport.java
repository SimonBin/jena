package org.apache.jena.dboe.storage.advanced.tuple;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamer;

public interface TupleQuerySupport<TupleType, ComponentType> {

     /** Method for running tuple queries */
     ResultStreamer<TupleType, ComponentType, Tuple<ComponentType>> find(TupleQuery<ComponentType> tupleQuery);

}
