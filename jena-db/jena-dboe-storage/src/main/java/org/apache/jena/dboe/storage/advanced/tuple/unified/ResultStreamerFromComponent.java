package org.apache.jena.dboe.storage.advanced.tuple.unified;

import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.Streamer;

public class ResultStreamerFromComponent<D, C>
    implements ResultStreamer<D, C, Tuple<C>>
{
    protected Streamer<?, C> componentStreamer;
    protected TupleAccessor<D, C> domainAccessor;

    public ResultStreamerFromComponent(Streamer<?, C> componentStreamer, TupleAccessor<D, C> domainAccessor) {
        super();
        this.componentStreamer = componentStreamer;
        this.domainAccessor = domainAccessor;
    }

    /**
     * Only works if the accessor can create domain objects with a single component
     */
    @Override
    public Stream<D> streamAsDomainObjects(Object store) {
        int domainDimension = domainAccessor.getRank();
        if (domainDimension != 1) {
            throw new UnsupportedOperationException("Cannot convert component into a domain object of dimension != 1; has" + domainDimension);
        }

        return streamAsComponent(store).map(
                // We checked that the dimension is 1, so we do not have to check that idx == 0
                component -> domainAccessor.restore(component, (x, idx) -> x));
    }

    @Override
    public Stream<C> streamAsComponent(Object store) {
        return componentStreamer.streamRaw(store);
    }

    @Override
    public Stream<Tuple<C>> streamAsTuple(Object store) {
        return streamAsComponent(store).map(TupleFactory::create1);
    }
}
