package org.apache.jena.dboe.storage.advanced.tuple.unified;

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;

public class ResultStreamerFromTuple<D, C>
    implements ResultStreamer<D, C, Tuple<C>>
{
    /** The dimension of the tuples returned by the tuple streamer */
    protected int tupleDimension;
    protected Supplier<Stream<Tuple<C>>> tupleStreamer;
    protected TupleAccessor<D, C> domainAccessor;

    public ResultStreamerFromTuple(
            int tupleDimension,
            Supplier<Stream<Tuple<C>>> tupleStreamer,
            TupleAccessor<D, C> domainAccessor) {
        super();
        this.tupleDimension = tupleDimension;
        this.tupleStreamer = tupleStreamer;
        this.domainAccessor = domainAccessor;
    }

    /**
     * Only works if the accessor can create domain objects with a single component
     */
    @Override
    public Stream<D> streamAsDomainObjects() {
        int domainDimension = domainAccessor.getRank();
        if (domainDimension != tupleDimension) {
            throw new UnsupportedOperationException("Tuple dimension " + tupleDimension + " does not match domain dimension " + domainDimension);
        }

        return streamAsTuple().map(tuple -> domainAccessor.restore(tuple, Tuple::get));
    }

    @Override
    public Stream<C> streamAsComponent() {
        if (tupleDimension != 1) {
            throw new UnsupportedOperationException("Cannot stream domain objects with dimension != 1 as a component");
        }

        return streamAsTuple().map(tuple -> tuple.get(0));
    }

    @Override
    public Stream<Tuple<C>> streamAsTuple() {
        return tupleStreamer.get();
    }
}