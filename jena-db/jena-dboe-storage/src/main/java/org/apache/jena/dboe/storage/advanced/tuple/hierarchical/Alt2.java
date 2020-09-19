package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

/**
 * This interface is conceptually just a pair just like Map.Entry
 * However, reusing Map.Entry for storage nodes (schema) is very confusing in debugging,
 * because Entry is especially used on the stores (often Maps).
 *
 *
 * @author raven
 *
 */
public class Alt2<V1, V2>
{
    protected V1 v1;
    protected V2 v2;

    public Alt2(V1 v1, V2 v2) {
        super();
        this.v1 = v1;
        this.v2 = v2;
    }

    public V1 getV1() {
        return v1;
    }

    public V2 getV2() {
        return v2;
    }


    public static <V1, V2> Alt2<V1, V2> create(V1 v1, V2 v2) {
        return new Alt2<>(v1, v2);
    }
}
