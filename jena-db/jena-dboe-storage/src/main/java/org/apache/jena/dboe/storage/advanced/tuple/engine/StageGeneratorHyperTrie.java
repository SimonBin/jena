package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.triple.TripleTableCore;
import org.apache.jena.dboe.storage.advanced.tuple.api.TupleAccessorTripleAnyToNull;
import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.EinsteinSummationFaster;
import org.apache.jena.dboe.storage.advanced.tuple.engine.faster.HyperTrieAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TupleCodec;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TupleCodecBased;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNode;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.core.StorageNodeBased;
import org.apache.jena.dboe.storage.advanced.tuple.tag.HasHyperTrieAccessor;
import org.apache.jena.ext.com.google.common.base.Stopwatch;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApply;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.util.iterator.ClosableIterator;
import org.apache.jena.util.iterator.WrappedIterator;

public class StageGeneratorHyperTrie
    implements StageGenerator
{
    protected boolean parallel = false;
    protected boolean bufferBindings = false;
    protected Consumer<String> bufferStatsCallback = null;

    protected boolean distinct;
    protected Set<Var> projection;

    public StageGeneratorHyperTrie() {
        super();
    }


    @Override
    protected StageGeneratorHyperTrie clone() {
        return StageGeneratorHyperTrie.create()
            .parallel(parallel)
            .bufferBindings(bufferBindings)
            .bufferStatsCallback(bufferStatsCallback)
            .distinct(distinct)
            .project(projection);
    }


    public static StageGeneratorHyperTrie create() {
        return new StageGeneratorHyperTrie();
    }

    public StageGeneratorHyperTrie distinct(boolean distinct) {
        this.distinct = distinct;
        return this;
    }


    public StageGeneratorHyperTrie project(Set<Var> projection) {
        this.projection = projection;
        return this;
    }

    public StageGeneratorHyperTrie parallel(boolean parallel) {
        this.parallel = parallel;
        return this;
    }


    /**
     * Buffer the bindings from the underlying stream in memory.
     * Used for debugging/profiling the performance overhead of getting the bindings here
     * directly vs passing them through the rest of the QueryIterator machinery
     *
     * @param bufferResult
     * @return
     */
    public StageGeneratorHyperTrie bufferBindings(boolean bufferResult) {
        this.bufferBindings = bufferResult;
        return this;
    }

    public StageGeneratorHyperTrie bufferStatsCallback(Consumer<String> bufferStatsCallback) {
        this.bufferStatsCallback = bufferStatsCallback;
        return this;
    }


    public static StorageNodeAndStoreAndCodec<?, ?> extractNodeAndStore(Graph graph) {
        StorageNodeAndStoreAndCodec<?, ?> result;

        if (graph instanceof GraphFromTripleTableCore) {
            GraphFromTripleTableCore g = (GraphFromTripleTableCore)graph;
            TripleTableCore ttc = g.getTripleTable();

            Object store;
            StorageNode<?, ?, ?> storageNode;
            TupleCodec<?, ?, ?, ?> tupleCodec = null;
            HyperTrieAccessor<?> hyperTrieAccessor = null;


            if (ttc instanceof StorageNodeBased) {

                // A graph based on a triple table must have Node objects as components
                @SuppressWarnings("unchecked")
                StorageNodeBased<?, ?, ?> snb = (StorageNodeBased<?, ?, ?>)ttc;

                storageNode = snb.getStorageNode();
                store = snb.getStore();
            } else {
                throw new RuntimeException("TripleTableCore does not implement StorageNodeBased");
            }

            if (ttc instanceof TupleCodecBased) {
                TupleCodecBased<?, ?, ?, ?> tmp = (TupleCodecBased<?, ?, ?, ?>)ttc;

                tupleCodec = tmp.getTupleCodec();
            }

            if (ttc instanceof HasHyperTrieAccessor) {
                HasHyperTrieAccessor<?> htb = (HasHyperTrieAccessor<?>)ttc;

                hyperTrieAccessor = htb.getHyperTrieAccessor();
            }

            result = new StorageNodeAndStoreAndCodec(storageNode, store, tupleCodec, hyperTrieAccessor);

        } else {
            throw new RuntimeException("Graph does not implement GraphFromTripleTableCore: " + graph.getClass());
        }

        return result;
    }

    @Override
    public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
        QueryIterator result = new QueryIterRepeatApply(input, execCxt) {
            @Override
            protected QueryIterator nextStage(Binding binding) {
                QueryIterBgpHyperTrie r = new QueryIterBgpHyperTrie(binding, pattern, execCxt, distinct, projection);
                return r;
            }
        };

        return result;
    }



    /**
     * Non-static inner class; shares several flags (parallel, distinct, etc)
     *
     * @author raven
     *
     */
    public class QueryIterBgpHyperTrie
        extends QueryIterMappingBase<BasicPattern>
    {
        public QueryIterBgpHyperTrie(
                Binding binding,
                BasicPattern templatePattern,
                ExecutionContext cxt,
                boolean distinct,
                Set<Var> projection
                ) {
            super(binding, templatePattern, cxt);
        }

        @Override
        protected BasicPattern substitute(BasicPattern pattern, Binding binding) {
            return Substitute.substitute(pattern, binding);
        }

        @Override
        protected ClosableIterator<Binding> initIterator() {
            Graph graph = getExecContext().getActiveGraph() ;
            StorageNodeAndStoreAndCodec<?, ?> storageAndStore = StageGeneratorHyperTrie.extractNodeAndStore(graph);

            boolean useNewApproach = true;

            Stream<Binding> stream = useNewApproach
                    ? newApproach(storageAndStore)
                    : oldApproach(storageAndStore);

            if (parallel) {
                stream = stream.parallel();
            }

            if (bufferBindings) {
                Stopwatch sw = Stopwatch.createStarted();
                List<Binding> buffer = stream.collect(Collectors.toList());
                if (bufferStatsCallback != null) {
                    bufferStatsCallback.accept("Buffered result set of size " + buffer.size() + " in " + sw);
                }

                stream = buffer.stream();
            }

            return WrappedIterator.create(stream.iterator());
        }



        public Stream<Binding> newApproach(StorageNodeAndStoreAndCodec<?, ?> storageAndStore) {
            Stream<Binding> stream;
            if (storageAndStore.getTupleCodec() == null) {
                stream = EinsteinSummationFaster.einsum(
                        (HyperTrieAccessor<Node>)storageAndStore.getHyperTrieAccessor(),
                        storageAndStore.getStore(),
                        pattern, distinct, projection);
            } else {

                TupleCodec<Triple, Node, Object, Object> codec = (TupleCodec<Triple, Node, Object, Object>)storageAndStore.getTupleCodec();

                stream = EinsteinSummationFaster.<Node, Object, Triple, Binding>einsumGeneric(
                        (HyperTrieAccessor<Object>)storageAndStore.getHyperTrieAccessor(),
                        storageAndStore.getStore(),
                        pattern,
                        TupleAccessorTripleAnyToNull.INSTANCE,
                        Node::isVariable,
                        codec::encodeComponent,
                        codec::decodeComponent,
                        distinct,
                        projection,
                        BindingFactory.root(),
                        (binding, varNode, valueNode) -> BindingFactory.binding(binding, (Var)varNode, valueNode));

            }
            return stream;
        }


        public Stream<Binding> oldApproach(StorageNodeAndStoreAndCodec<?, ?> storageAndStore) {
            Stream<Binding> stream;
            if (storageAndStore.getTupleCodec() == null) {
                stream = EinsteinSummation.einsum(
                        (StorageNode<?, Node, ?>)storageAndStore.getStorage(),
                        storageAndStore.getStore(),
                        pattern, distinct, projection);
            } else {

                TupleCodec<Triple, Node, Object, Object> codec = (TupleCodec<Triple, Node, Object, Object>)storageAndStore.getTupleCodec();

                stream = EinsteinSummation.<Node, Object, Triple, Binding>einsumGeneric(
                        (StorageNode<?, Object, ?>)storageAndStore.getStorage(),
                        storageAndStore.getStore(),
                        pattern,
                        TupleAccessorTripleAnyToNull.INSTANCE,
                        Node::isVariable,
                        codec::encodeComponent,
                        codec::decodeComponent,
                        distinct,
                        projection,
                        BindingFactory.root(),
                        (binding, varNode, valueNode) -> BindingFactory.binding(binding, (Var)varNode, valueNode));

            }
            return stream;
        }
    }

}
