package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.stream.Stream;

import org.apache.jena.dboe.storage.advanced.triple.TripleTableCore;
import org.apache.jena.dboe.storage.advanced.tuple.api.TupleAccessorTriple;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

public class GraphFromTripleTableCore
    extends GraphBase
    implements AdvancedTripleFind
{
    protected TripleTableCore tripleTable;

    public GraphFromTripleTableCore(TripleTableCore tripleTable) {
        super();
        this.tripleTable = tripleTable;
    }

    public TripleTableCore getTripleTable() {
        return tripleTable;
    }

    public static GraphFromTripleTableCore create(TripleTableCore tripleTable) {
        return new GraphFromTripleTableCore(tripleTable);
    }

    @Override
    public void add(Triple t) {
        tripleTable.add(t);
    }


    @Override
    public void remove(Node s, Node p, Node o) {
        for (Triple t : find(s, p, o).toList()) {
            tripleTable.delete(t);
        }
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        Stream<Triple> stream = tripleTable
                .find(triplePattern.getMatchSubject(), triplePattern.getMatchPredicate(), triplePattern.getMatchObject());

        return WrappedIterator.create(stream.iterator());
    }

    @Override
    public Stream<Binding> find(boolean distinct, Node s, Node p, Node o) {
        return TupleExecutor.findTriple(distinct, new Triple(s, p, o), TupleAccessorTriple.INSTANCE, tripleTable);
    }
}
