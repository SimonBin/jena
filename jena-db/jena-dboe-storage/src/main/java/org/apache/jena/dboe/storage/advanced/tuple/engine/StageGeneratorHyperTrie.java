package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.triple.TripleTableCore;
import org.apache.jena.dboe.storage.advanced.triple.TripleTableFromStorageNodeWithCodec;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTripleAnyToNull;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNode;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeBased;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.TupleCodec;
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

    public StageGeneratorHyperTrie() {
        super();
    }


    public static StageGeneratorHyperTrie create() {
        return new StageGeneratorHyperTrie();
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

            if (ttc instanceof StorageNodeBased) {

                // A graph based on a triple table must have Node objects as components
                @SuppressWarnings("unchecked")
                StorageNodeBased<?, ?, ?> snb = (StorageNodeBased<?, ?, ?>)ttc;

                result = new StorageNodeAndStoreAndCodec<>(snb.getStorageNode(), snb.getStore(), null);
            } else if (ttc instanceof TripleTableFromStorageNodeWithCodec) {
                TripleTableFromStorageNodeWithCodec<?, ?, ?> tmp = (TripleTableFromStorageNodeWithCodec<?, ?, ?>)ttc;

                result = new StorageNodeAndStoreAndCodec(tmp.getStorageNode(), tmp.getStore(), tmp.getTupleCodec());

            } else {
                throw new RuntimeException("TripleTableCore does not implement StorageNodeBased");
            }


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
                QueryIterator r = new QueryIterBgpHyperTrie(binding, pattern, execCxt);
                return r;
            }
        };

        return result;
    }


    /**
     * Non-static inner class; shares the parallel flag
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
                ExecutionContext cxt) {
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

            Stream<Binding> stream;
            if (storageAndStore.getTupleCodec() == null) {
                stream = EinsteinSummation.einsum(
                        (StorageNode<?, Node, ?>)storageAndStore.getStorage(),
                        storageAndStore.getStore(),
                        pattern, null);
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
                        BindingFactory.root(),
                        (binding, varNode, valueNode) -> BindingFactory.binding(binding, (Var)varNode, valueNode));

            }

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
    }

}
