package org.apache.jena.dboe.storage.tuple;

import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;


/**
 * This is a generic base class for tuple-like access to any collection of any domain objects.
 *
 * The domain object can be of any type (e.g. Triple or Quad). All that is needed is a corresponding
 * TupleAccessor instance that allows extracting components of a domain object with accessor.get(triple, 0).
 *
 * The number of components (called the rank) an accessor can yield for a domain object must be constant.
 *
 * This class enables keeping track of 3 aspects:
 * <ul>
 *   <li>Equality restrictions on components (may be extended to range constraints in the future)</li>
 *   <li>Distinct (yes or no)</li>
 *   <li>Projection; this requires mapping domain objects to tuples</li>
 * <ul>
 *
 * This class provides a unified interface for accessing distinct values.
 * For example, obtaining the distinct graphs via datasetGraph.find() requires a scan of all triples.
 * With this class it is possible to express
 * <pre>
 * Stream<Node> graphNodeStream = quadTable
 *   .newTupleFinder()
 *   .project(3) // The graph component has index 3 in order to be consistent with Triple
 *   .distinct()
 *   .plain() // Instead of wrapping values in Tuple1 instances yield the values themselves
 *   .stream()
 *
 * </pre>
 *
 * Default implementations can simply delegate back to the usual .find() methods, however
 * specializations may choose to perform optimized executions.
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <ExposedType>
 * @param <DomainType>
 * @param <ComponentType>
 */
public class TupleFinderBase<ExposedType, DomainType, ComponentType>
    implements TupleFinder<ExposedType, DomainType, ComponentType>
{
    /**
     * The tupleTable this finder is attached to
     *
     */
    protected TupleTableCore<DomainType, ComponentType> tupleTable;

    /**
     * The instance of the underlying tuple query
     *
     */
    protected TupleQuery<ComponentType> tupleQuery;

    public static interface Strategy<ExposedType, DomainType, ComponentType> {
        Stream<ExposedType> exec(TupleTableCore<DomainType, ComponentType> tupleTable, TupleQuery<ComponentType> query);
    }

    /**
     * The current strategy to create the stream
     *
     *
     */
    protected Strategy<ExposedType, DomainType, ComponentType> strategy;


    /**
     * Back and forth conversion of domain types (e.g. triples/quads) to Jena tuples
     */
//    protected ConverterTuple<DomainType, ComponentType> tupleConverter;

//
//    /**
//     * Tuple-like access to the internal objects
//     */
//    protected TupleAccessor<DomainType, ComponentType> internalAccessor;
//
//
//    /**
//     * Tuple-like access to the exposed objects
//     *
//     */
//    protected TupleAccessor<ExposedType, ComponentType> exposedAccessor;


    @SuppressWarnings("unchecked")
    public static <DomainType, ComponentType> TupleFinder<DomainType, DomainType, ComponentType> create(
            TupleTableCore<DomainType, ComponentType> tupleTable) {

        return new TupleFinderBase<DomainType, DomainType, ComponentType>(
                    tupleTable,
                    new TupleQueryImpl<>(tupleTable.getRank()),
                    // Default strategy with a fresh TupleQueryImpl is to pass a null-value-filled
                    // pattern to the findTuple methods
                    (table, query) -> table.findTuples(TupleQueryImpl.listOfNulls(tupleTable.getRank())));
    }

    public TupleFinderBase(
            TupleTableCore<DomainType, ComponentType> tupleTable,
            TupleQuery<ComponentType> tupleQuery,
            Strategy<ExposedType, DomainType, ComponentType> strategy) {
        super();
        this.tupleTable = tupleTable;
        this.tupleQuery = tupleQuery;
        this.strategy = strategy;
    }



    public <NewExposedType> TupleFinder<NewExposedType, DomainType, ComponentType>
    newProjectedTupleFinder(Strategy<NewExposedType, DomainType, ComponentType> newStrategy) {
        return new TupleFinderBase<NewExposedType, DomainType, ComponentType>(tupleTable, tupleQuery, newStrategy);
    }

    @Override
    public int getBaseRank() {
        return tupleTable.getRank();
    }

    public void checkIndex(int idx) {
        if (idx < 0 || idx >= getBaseRank()) {
            throw new IndexOutOfBoundsException("" + idx);
        }
    }

    @Override
    public TupleFinder<ExposedType, DomainType, ComponentType> eq(int componentIdx, ComponentType value) {
        tupleQuery.setConstraint(componentIdx, value);
        return this;
    }

    @Override
    public TupleFinder<ExposedType, DomainType, ComponentType> distinct(boolean onOrOff) {
        tupleQuery.setDistinct(onOrOff);
        return this;
    }


    /**
     * Can only be called if exactly one component is projected
     *
     *
     */
    @Override
    public TupleFinder<ComponentType, DomainType, ComponentType> plain() {
        return newProjectedTupleFinder((table, query) -> table.find(query).map(t -> t.get(0)));
    }

    @Override
    public TupleFinder<Tuple<ComponentType>, DomainType, ComponentType> tuples() {
        return newProjectedTupleFinder((table, query) -> table.find(query));
    }


    @Override
    public TupleFinder<Tuple<ComponentType>, DomainType, ComponentType> project(int... componentIdx) {
        tupleQuery.setProject(componentIdx);
        return tuples();
    }


    @Override
    public boolean canAs(Class<?> viewClass) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <T> T as(Class<T> viewClass) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Stream<ExposedType> stream() {
        return strategy.exec(tupleTable, tupleQuery);
    }

    @Override
    public int weight() {
        return 0;
    }
}
