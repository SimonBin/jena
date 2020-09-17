package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Helper interface for creating new map instances with automatically inferred types
 *
 * @author Claus Stadler 11/09/2020
 *
 */
@FunctionalInterface
public interface MapSupplier {
    <K, V> Map<K, V> get();


    public static MapSupplier forTreeMap(Comparator<?> cmp) {
        return new MapSupplierTreeMap(cmp);
    }

    public static class MapSupplierTreeMap
        implements MapSupplier
    {
        protected Comparator<?> cmp;

        public MapSupplierTreeMap(Comparator<?> cmp) {
            super();
            this.cmp = cmp;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public <K, V> Map<K, V> get() {
           return new TreeMap<K, V>((Comparator)cmp);
        }
    }
}
