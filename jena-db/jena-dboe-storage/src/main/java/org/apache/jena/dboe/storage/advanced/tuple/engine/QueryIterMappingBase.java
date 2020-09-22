package org.apache.jena.dboe.storage.advanced.tuple.engine;

import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterTriplePattern;
import org.apache.jena.util.iterator.ClosableIterator;
import org.apache.jena.util.iterator.NiceIterator;


/**
 * An adaption of {@link QueryIterTriplePattern} that generalizes to arbitrary
 * patterns that need to be substituted on each input binding.
 *
 *
 * @author raven
 *
 * @param <P>
 */
public abstract class QueryIterMappingBase<P> extends QueryIter
    {
        protected P pattern;
        protected Binding binding ;
        protected ClosableIterator<Binding> graphIter ;
        protected Binding slot = null ;
        protected boolean finished = false ;
        protected volatile boolean cancelled = false ;

        protected abstract P substitute(P pattern, Binding binding);
        protected abstract ClosableIterator<Binding> initIterator();

        public QueryIterMappingBase(
                Binding binding,
                P templatePattern,
                ExecutionContext cxt)
        {
            super(cxt) ;

            this.pattern = substitute(templatePattern, binding);
            this.binding = binding ;
            this.graphIter = initIterator();
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
                BindingMap results = BindingFactory.create(binding) ;
                results.addAll(t);

                slot = results; //mapper(t) ;
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