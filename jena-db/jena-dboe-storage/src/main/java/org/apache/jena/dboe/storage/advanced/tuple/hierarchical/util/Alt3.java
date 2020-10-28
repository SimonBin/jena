package org.apache.jena.dboe.storage.advanced.tuple.hierarchical.util;

public class Alt3<V1, V2, V3> {
    protected V1 v1;
    protected V2 v2;
    protected V3 v3;

    public Alt3(V1 v1, V2 v2, V3 v3) {
        super();
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
    }

    public V1 getV1() {
        return v1;
    }

    public V2 getV2() {
        return v2;
    }

    public V3 getV3() {
        return v3;
    }

    public static <V1, V2, V3> Alt3<V1, V2, V3> create(V1 v1, V2 v2, V3 v3) {
        return new Alt3<>(v1, v2, v3);
    }

}
