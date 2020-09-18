package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApply;
import org.apache.jena.util.iterator.ClosableIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;


public class QueryIterTriplePatternFromTuple extends QueryIterRepeatApply
{
    private final boolean distinct;
    private final Triple pattern ;

    public QueryIterTriplePatternFromTuple( QueryIterator input,
            boolean distinct,
                                   Triple pattern ,
                                   ExecutionContext cxt)
    {
        super(input, cxt) ;
        this.distinct = distinct;
        this.pattern = pattern ;
    }

    @Override
    protected QueryIterator nextStage(Binding binding)
    {
        return new TripleMapper(distinct, binding, pattern, getExecContext()) ;
    }

    static int countMapper = 0 ;
    static class TripleMapper extends QueryIter
    {
        private boolean distinct;

        private Node s ;
        private Node p ;
        private Node o ;
        private Binding binding ;
        private ClosableIterator<Binding> graphIter ;
        private Binding slot = null ;
        private boolean finished = false ;
        private volatile boolean cancelled = false ;

        TripleMapper(boolean distinct, Binding binding, Triple pattern, ExecutionContext cxt)
        {
            super(cxt) ;

            this.distinct = distinct;

            this.s = substitute(pattern.getSubject(), binding) ;
            this.p = substitute(pattern.getPredicate(), binding) ;
            this.o = substitute(pattern.getObject(), binding) ;
            this.binding = binding ;
            Graph graph = cxt.getActiveGraph() ;

            AdvancedTripleFind finder = (AdvancedTripleFind)graph;
            System.out.println("Lookup with " + Arrays.asList(distinct, s, p, o));
            Stream<Binding> tmp = finder.find(distinct, s, p, o);

            this.graphIter = WrappedIterator.create(tmp.iterator()); //graph.find(s2, p2, o2) ;
        }

        private static Node tripleNode(Node node)
        {
            if ( node.isVariable() )
                return Node.ANY ;
            return node ;
        }

        private static Node substitute(Node node, Binding binding)
        {
            if ( Var.isVar(node) )
            {
                Node x = binding.get(Var.alloc(node)) ;
                if ( x != null )
                    return x ;
            }
            return node ;
        }

        private Binding mapper(Triple r)
        {
            BindingMap results = BindingFactory.create(binding) ;

            if ( ! insert(s, r.getSubject(), results) )
                return null ;
            if ( ! insert(p, r.getPredicate(), results) )
                return null ;
            if ( ! insert(o, r.getObject(), results) )
                return null ;
            return results ;
        }

        private static boolean insert(Node inputNode, Node outputNode, BindingMap results)
        {
            if ( ! Var.isVar(inputNode) )
                return true ;

            Var v = Var.alloc(inputNode) ;
            Node x = results.get(v) ;
            if ( x != null )
                return outputNode.equals(x) ;

            results.add(v, outputNode) ;
            return true ;
        }

        @Override
        protected boolean hasNextBinding()
        {
            if ( finished ) return false ;
            if ( slot != null ) return true ;
            if ( cancelled )
            {
                graphIter.close() ;
                finished = true ;
                return false ;
            }

            while(graphIter.hasNext() && slot == null )
            {
                Binding t = graphIter.next() ;
                slot = t; //mapper(t) ;
            }
            if ( slot == null )
                finished = true ;
            return slot != null ;
        }

        @Override
        protected Binding moveToNextBinding()
        {
            if ( ! hasNextBinding() )
                throw new ARQInternalErrorException() ;
            Binding r = slot ;
            slot = null ;
            return r ;
        }

        @Override
        protected void closeIterator()
        {
            if ( graphIter != null )
                NiceIterator.close(graphIter) ;
            graphIter = null ;
        }

        @Override
        protected void requestCancel()
        {
            // The QueryIteratorBase machinary will do the real work.
            cancelled = true ;
        }
    }
}