package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Set;

/**
 * Helper interface for creating new set instances with automatically inferred types
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public interface SetSupplier {
  <V> Set<V> get();
}
