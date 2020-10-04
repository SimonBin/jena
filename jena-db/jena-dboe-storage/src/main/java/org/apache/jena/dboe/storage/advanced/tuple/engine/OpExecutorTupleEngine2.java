package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;

/**
 * The problem with the ordinary OpExecutor is that it cannot pass DISTINCT and
 * projection information to the stage generator.
 *
 *
 * @author raven
 *
 */
public class OpExecutorTupleEngine2 extends OpExecutor {

    public final static OpExecutorFactory opExecFactory = new OpExecutorFactory()
    {
        @Override
        public OpExecutor create(ExecutionContext execCxt)
        {
            return new OpExecutorTupleEngine2(execCxt) ;
        }
    };

    private final boolean isForTuple;

    protected OpExecutorTupleEngine2(ExecutionContext execCtx) {
        super(execCtx);

        isForTuple = execCtx.getActiveGraph() instanceof GraphFromTripleTableCore ;
    }


    // does not work in general; we have to see how we can flag a bgp that it should be executed in distinct mode
    protected boolean HACK_distinctSeen = false;
    protected Set<Var> HACK_projectSeen = null;

    @Override
    protected QueryIterator execute(OpProject opProject, QueryIterator input) {
        this.HACK_projectSeen = new LinkedHashSet<>(opProject.getVars());
//        return super.execute(opProject, input);
        return exec(opProject.getSubOp(), input);
    }

    @Override
    protected QueryIterator execute(OpDistinct opDistinct, QueryIterator input) {
        HACK_distinctSeen = true;
//        return super.execute(opDistinct, input);
        return exec(opDistinct.getSubOp(), input);
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
        return executeBgp(opBGP.getPattern(), input);
    }


    protected QueryIterator executeBgp(BasicPattern bgp, QueryIterator input) {
        StageGenerator tmp = StageBuilder.getGenerator(execCxt.getContext());
        StageGeneratorHyperTrie sg = (StageGeneratorHyperTrie)tmp;

        QueryIterator result = sg.clone()
            .distinct(HACK_distinctSeen)
            .project(HACK_projectSeen)
            .execute(bgp, input, execCxt)
            ;

        return result;
    }

}
