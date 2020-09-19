package org.apache.jena.dboe.storage.advanced.tuple.engine;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderLib;

public class OpExecutorTupleEngine extends OpExecutor {

    public final static OpExecutorFactory opExecFactory = new OpExecutorFactory()
    {
        @Override
        public OpExecutor create(ExecutionContext execCxt)
        {
            return new OpExecutorTupleEngine(execCxt) ;
        }
    };

    private final boolean isForTuple;

    protected OpExecutorTupleEngine(ExecutionContext execCtx) {
        super(execCtx);

        isForTuple = execCtx.getActiveGraph() instanceof AdvancedTripleFind ;
    }


    // does not work in general; we have to see how we can flag a bgp that it should be executed in distinct mode
    protected boolean HACK_distinctSeen = false;

    @Override
    protected QueryIterator execute(OpDistinct opDistinct, QueryIterator input) {
        HACK_distinctSeen = true;
        return super.execute(opDistinct, input);
    }


//    @Override
//    protected QueryIterator execute(OpGraph opGraph, QueryIterator input) {
//        this.execCxt.getDataset()
//
//
//        return super.execute(opGraph, input);
//    }

    @Override
    protected QueryIterator execute(OpBGP opBGP, QueryIterator input) {
        return executeBgp(HACK_distinctSeen, opBGP, input);
    }


    protected QueryIterator executeBgp(boolean distinct, OpBGP opBGP, QueryIterator input) {
        BasicPattern pattern = opBGP.getPattern() ;
        return createIter(distinct, pattern, input, execCxt);
    }


    public QueryIterator createIter(
            boolean distinct,
            BasicPattern pattern,
            QueryIterator input,
            ExecutionContext execCxt) {

//        BasicPattern reordered = pattern;
        BasicPattern reordered = ReorderLib.fixed().reorder(pattern);

        QueryIterator chain = input;
        for (Triple triple : reordered) {
            chain = new QueryIterTriplePatternFromTuple(chain, distinct, triple, execCxt) ;
        }

        return chain;
    }

}
