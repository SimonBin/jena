package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.ResultStreamer;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleOps;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Streamer;

public class ResultStreamerFromDomain<D, C>
    implements ResultStreamer<D, C, Tuple<C>>
{
    protected Streamer<?, D> domainStreamer;
    protected TupleAccessor<D, C> domainAccessor;

    public ResultStreamerFromDomain(Streamer<?, D> domainStreamer, TupleAccessor<D, C> domainAccessor) {
        super();
        this.domainStreamer = domainStreamer;
        this.domainAccessor = domainAccessor;
    }

    @Override
    public Stream<D> streamAsDomainObjects(Object store) {
        return domainStreamer.streamRaw(store);
    }

    @Override
    public Stream<C> streamAsComponent(Object store) {
        if (domainAccessor.getRank() != 1) {
            throw new UnsupportedOperationException("Cannot stream domain objects with dimension != 1 as a component");
        }

        return streamAsDomainObjects(store).map(item -> domainAccessor.get(item,0));
    }

    @Override
    public Stream<Tuple<C>> streamAsTuple(Object store) {
        Function<D, Tuple<C>> tupelizer = TupleOps.tupelizer(domainAccessor);
        return domainStreamer.streamRaw(store).map(tupelizer);
    }
}
