package org.apache.jena.dboe.storage.advanced.tuple;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TupleQueryImpl<ComponentType>
    implements TupleQuery<ComponentType>
{
    protected int dimension;
    protected List<ComponentType> pattern;
    protected boolean distinct = false;
    protected int[] projection = null;

//    public static <C> TupleQuery<C> create(int dimension) {
//        return new TupleQueryImpl<>(dimension);
//    }

    public static <T> List<T> listOfNulls(int size) {
        List<T> result = new ArrayList<>();
        for (int i = 0; i < size; ++i) { result.add(null); }
        return result;
    }

    public TupleQueryImpl(int dimension) {
        super();
        this.dimension = dimension;
        this.pattern = listOfNulls(dimension);
    }

    @Override
    public int getDimension() {
        return dimension;
    }

    @Override
    public TupleQuery<ComponentType> setDistinct(boolean onOrOff) {
        this.distinct = onOrOff;
        return this;
    }

    @Override
    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public TupleQuery<ComponentType> setConstraint(int idx, ComponentType value) {
        pattern.set(idx, value);
        return this;
    }

    @Override
    public ComponentType getConstraint(int idx) {
        return pattern.get(idx);
    }

    @Override
    public int[] getProject() {
        return projection;
    }

    @Override
    public List<ComponentType> getPattern() {
        return pattern;
    }

    @Override
    public TupleQuery<ComponentType> setProject(int... tupleIdxs) {
        projection = tupleIdxs;
        return this;
    }

    @Override
    public boolean hasProject() {
        return projection != null;
    }

    @Override
    public Set<Integer> getConstrainedComponents() {
        Set<Integer> result = new LinkedHashSet<Integer>();
        for (int i = 0; i < dimension; ++i) {
            ComponentType value = getConstraint(i);

            // FIXME Check for 'ANY'
            if (value != null) {
                result.add(i);
            }
        }
        return result;
    }


    @Override
    public String toString() {
        String result
            = (isDistinct() ? "DISTINCT " : "")
            + (projection == null
                ? "*"
                : IntStream.of(projection)
                    .mapToObj(Integer::toString).collect(Collectors.joining(" ")))
            + " WHERE "
            + (getConstrainedComponents().isEmpty() ? "TRUE" :
                getConstrainedComponents().stream().map(idx -> "" + idx + "=" + getConstraint(idx))
                .collect(Collectors.joining(" AND ")));

        return result;
    }
}
