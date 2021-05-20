package org.apache.jena.dboe.storage.advanced.tuple.constraint;

/**
 * A constraint on a tuple's component.
 * StorageNodes may be able to perform better than O(n)
 * when answering items that match the constraints,
 * For example, sorted stores may support range lookups. 
 * 
 * The {@link #eval(Object)} method acts as a generic fallback. 
 * 
 * @param The component type
 * 
 * @author Claus Stadler
 *
 */
@FunctionalInterface
public interface Constraint<C> {
	boolean test(C testValue);
}
