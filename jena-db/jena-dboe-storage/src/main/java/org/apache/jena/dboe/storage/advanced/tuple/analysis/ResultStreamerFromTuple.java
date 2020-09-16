package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.ResultStreamer;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Streamer;

public class ResultStreamerFromTuple<D, C>
    implements ResultStreamer<D, C, Tuple<C>>
{
    /** The dimension of the tuples returned by the tuple streamer */
    protected int tupleDimension;
    protected Streamer<?, Tuple<C>> tupleStreamer;
    protected TupleAccessor<D, C> domainAccessor;

    public ResultStreamerFromTuple(
            int tupleDimension,
            Streamer<?, Tuple<C>> tupleStreamer,
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
    public Stream<D> streamAsDomainObjects(Object store) {
        int domainDimension = domainAccessor.getRank();
        if (domainDimension != tupleDimension) {
            throw new UnsupportedOperationException("Tuple dimension " + tupleDimension + " does not match domain dimension " + domainDimension);
        }

        return streamAsTuple(store).map(tuple -> domainAccessor.restore(tuple, Tuple::get));
    }

    @Override
    public Stream<C> streamAsComponent(Object store) {
        if (tupleDimension != 1) {
            throw new UnsupportedOperationException("Cannot stream domain objects with dimension != 1 as a component");
        }

        return streamAsTuple(store).map(tuple -> tuple.get(0));
    }

    @Override
    public Stream<Tuple<C>> streamAsTuple(Object store) {
        return tupleStreamer.streamRaw(store);
    }
}