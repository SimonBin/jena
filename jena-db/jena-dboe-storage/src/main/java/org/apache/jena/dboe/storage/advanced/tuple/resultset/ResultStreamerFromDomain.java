package org.apache.jena.dboe.storage.advanced.tuple.resultset;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleOps;
import org.apache.jena.sparql.core.Quad;


/**
 * Implementation backed by a supplier of streams of domain objects such as {@link Quad}s.
 * Can convert to tuple and component representation (if applicable).
 * Domain objects can only be converted to component representation if they are logical 1-tuples.
 *
 * @author raven
 *
 * @param <D> The domain type such as Quad
 * @param <C> The component type such as Node
 */
public class ResultStreamerFromDomain<D, C>
    implements ResultStreamer<D, C, Tuple<C>>
{
    protected Supplier<Stream<D>> domainStreamer;
    protected TupleAccessor<D, C> domainAccessor;

    public ResultStreamerFromDomain(Supplier<Stream<D>> domainStreamer, TupleAccessor<D, C> domainAccessor) {
        super();
        this.domainStreamer = domainStreamer;
        this.domainAccessor = domainAccessor;
    }

    @Override
    public Stream<D> streamAsDomainObject() {
        return domainStreamer.get();
    }

    @Override
    public Stream<C> streamAsComponent() {
        if (domainAccessor.getRank() != 1) {
            throw new UnsupportedOperationException("Cannot stream domain objects with dimension != 1 as a component");
        }

        return streamAsDomainObject().map(item -> domainAccessor.get(item,0));
    }

    @Override
    public Stream<Tuple<C>> streamAsTuple() {
        Function<D, Tuple<C>> tupelizer = TupleOps.tupelizer(domainAccessor);
        return streamAsDomainObject().map(tupelizer);
    }
}
