package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;

public interface TupleCodec<D1, C1, D2, C2> {

    C2 encodeComponent(C1 c1);

    C1 decodeComponent(C2 c2);

    D2 encodeTuple(D1 sourceTuple);

    D1 decodeTuple(D2 targetTuple);

    TupleAccessor<D1, C1> getSourceTupleAccessor();

    TupleAccessor<D2, C2> getTargetTupleAccessor();

    /**
     * This method can be used as a TupleAccessorCore
     *
     * @param d1
     * @param idx
     * @return
     */
    C2 getEncodedComponent(D1 d1, int idx);

    /**
     * This method can be used as a TupleAccessorCore
     *
     * @param d1
     * @param idx
     * @return
     */
    C1 getDecodedComponent(D2 d2, int idx);
}
