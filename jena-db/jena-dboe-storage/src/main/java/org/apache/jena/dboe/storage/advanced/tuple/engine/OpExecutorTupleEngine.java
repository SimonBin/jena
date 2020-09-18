package org.apache.jena.dboe.storage.advanced.tuple.engine;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;

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


    @Override
    protected QueryIterator execute(OpDistinct opDistinct, QueryIterator input) {
        // TODO Auto-generated method stub
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
        return executeBgp(false, opBGP, input);
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

        QueryIterator chain = input;
        for (Triple triple : pattern) {
            chain = new QueryIterTriplePatternFromTuple(chain, distinct, triple, execCxt) ;
        }

        return chain;
    }

}
