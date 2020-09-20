package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.triple.TripleTableCore;
import org.apache.jena.dboe.storage.advanced.tuple.hierarchical.StorageNodeBased;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApply;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.util.iterator.ClosableIterator;
import org.apache.jena.util.iterator.WrappedIterator;

public class StageGeneratorHyperTrie
    implements StageGenerator
{
    protected boolean parallel = false;

    public StageGeneratorHyperTrie(boolean parallel) {
        super();
        this.parallel = parallel;
    }

    public static StorageNodeAndStore<?, Node> extractNodeAndStore(Graph graph) {
        StorageNodeAndStore<?, Node> result;

        if (graph instanceof GraphFromTripleTableCore) {
            GraphFromTripleTableCore g = (GraphFromTripleTableCore)graph;
            TripleTableCore ttc = g.getTripleTable();

            if (ttc instanceof StorageNodeBased) {

                // A graph based on a triple table must have Node objects as components
                @SuppressWarnings("unchecked")
                StorageNodeBased<?, Node, ?> snb = (StorageNodeBased<?, Node, ?>)ttc;

                result = new StorageNodeAndStore<>(snb.getStorageNode(), snb.getStore());

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
            StorageNodeAndStore<?, Node> storageAndStore = StageGeneratorHyperTrie.extractNodeAndStore(graph);

            Stream<Binding> stream = EinsteinSummation.einsum(
                    storageAndStore.getStorage(), storageAndStore.getStore(),
                    pattern, null);

            if (parallel) {
                stream = stream.parallel();
            }

            return WrappedIterator.create(stream.iterator());
        }
    }

}
