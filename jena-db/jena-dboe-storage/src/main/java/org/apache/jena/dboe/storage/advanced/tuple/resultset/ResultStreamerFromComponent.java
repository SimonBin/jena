package org.apache.jena.dboe.storage.advanced.tuple.resultset;

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.graph.Node;

/**
 * Implementation backed by a supplier of streams of components such as {@link Node}s.
 * Can convert to domain and tuple representation (if applicable).
 *
 * @author raven
 *
 * @param <D>
 * @param <C>
 */
public class ResultStreamerFromComponent<D, C>
    implements ResultStreamer<D, C, Tuple<C>>
{
    protected Supplier<Stream<C>> componentStreamer;
    protected TupleAccessor<D, C> domainAccessor;

    public ResultStreamerFromComponent(Supplier<Stream<C>> componentStreamer, TupleAccessor<D, C> domainAccessor) {
        super();
        this.componentStreamer = componentStreamer;
        this.domainAccessor = domainAccessor;
    }

    /**
     * Only works if the accessor can create domain objects with a single component
     */
    @Override
    public Stream<D> streamAsDomainObjects() {
        int domainDimension = domainAccessor.getRank();
        if (domainDimension != 1) {
            throw new UnsupportedOperationException("Cannot convert component into a domain object of dimension != 1; has" + domainDimension);
        }

        return streamAsComponent().map(
                // We checked that the dimension is 1, so we do not have to check that idx == 0
                component -> domainAccessor.restore(component, (x, idx) -> x));
    }

    @Override
    public Stream<C> streamAsComponent() {
        return componentStreamer.get();
    }

    @Override
    public Stream<Tuple<C>> streamAsTuple() {
        return streamAsComponent().map(TupleFactory::create1);
    }
}
