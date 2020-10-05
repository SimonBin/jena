package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

public interface TupleCodecBased<D1, C1, D2, C2> {
    TupleCodec<D1, C1, D2, C2> getTupleCodec();
}
