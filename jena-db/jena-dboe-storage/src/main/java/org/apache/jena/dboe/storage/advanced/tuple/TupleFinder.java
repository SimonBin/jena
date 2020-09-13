package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;

public interface TupleFinder<ExposedType, DomainType, ComponentType> {

    /**
     * The number of columns of the underlying tuple source
     *
     * @return
     */
    int getBaseRank();

    /**
     * Set an equals constraint on a component
     *
     * @param componentIdx
     * @param value
     * @return
     */
//    TupleFinder<DomainType, ComponentType> eq(int componentIdx, ComponentType value);
//
//    TupleFinder<DomainType, ComponentType> distinct(boolean onOrOff);

    /**
     * If the tuple finder is already list-based to nothing
     * Otherwise
     *
     * @return
     */
    TupleFinder<Tuple<ComponentType>, DomainType, ComponentType> tuples();

    TupleFinder<ComponentType, DomainType, ComponentType> plain();

    TupleFinder<ExposedType, DomainType, ComponentType> distinct(boolean OnOrOff);

    default TupleFinder<ExposedType, DomainType, ComponentType> distinct() {
        return distinct(true);
    }

    /**
     * Add a specific component to the projection
     *
     * @param componentIdx
     * @return
     */
    TupleFinder<Tuple<ComponentType>, DomainType, ComponentType> project(int... componentIdx);

    /**
     * Remove a component from the projection
     *
     * @param componentIdx
     * @return
     */
//    TupleFinder<Tuple<ComponentType>, ComponentType> unproject(int componentIdx);

    /**
     * Whether the current TupleType can be viewed as instance of another class.
     * For example, any List<Node> with three components can be viewed as a Triple.
     *
     * @param viewClass
     * @return
     */
    boolean canAs(Class<?> viewClass);

    <T> T as(Class<T> viewClass);


    /**
     * Weight the current state of the TupleFinder as an estimate of the amount
     * of work required for retrieval execution
     *
     * See also {@link TupleIndex}
     *
     * @return
     */
    int weight();

    /**
     * Execute the request
     *
     * @return
     */
    // ExtendedIterator<DomainType> exec();

    Stream<ExposedType> stream();

    TupleFinder<ExposedType, DomainType, ComponentType> eq(int componentIdx, ComponentType value);

    default TupleFinder<ComponentType, DomainType, ComponentType> projectOnly(int componentIdx) {
        return project(componentIdx).plain();
    }

}