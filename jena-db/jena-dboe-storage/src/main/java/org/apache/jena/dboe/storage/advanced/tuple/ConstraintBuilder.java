package org.apache.jena.dboe.storage.advanced.tuple;

/**
 * Configure constraints for eventually calling build()
 * which will create the node that can provide statistics for the given constraints
 *
 *
 * @author raven
 *
 */
public interface ConstraintBuilder<ComponentType> {
    IndexNode<ComponentType> getParent();
    ConstraintBuilder<ComponentType> set(@SuppressWarnings("unchecked") ComponentType ...componentTypes);
    IndexNodeFork<ComponentType> build();
}
