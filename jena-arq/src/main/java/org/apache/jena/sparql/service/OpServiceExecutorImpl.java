package org.apache.jena.sparql.service;

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.query.QueryExecException;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterSingleton;
import org.apache.jena.sparql.util.Context;


/** Helper class to simplify executing concrete OpService instances */
public class OpServiceExecutorImpl
	implements OpServiceExecutor
{
	// protected boolean isSilent;
	protected OpService originalOp;

	protected boolean silent;
	protected ExecutionContext execCxt;
	protected Context cxt;
	protected ServiceExecutorRegistry registry;

	public OpServiceExecutorImpl(OpService opService, ExecutionContext execCxt) {
		this.originalOp = opService;
		this.silent = opService.getSilent();
		this.execCxt = execCxt;
        this.cxt = execCxt.getContext();
        this.registry = ServiceExecutorRegistry.get(cxt);
	}

	public QueryIterator exec(OpService substitutedOp) {

		Binding input = BindingFactory.binding();

        ServiceExecution svcExec = null;
		try {
            // ---- Find handler
            if ( registry != null ) {
                for ( ServiceExecutorFactory factory : registry.getFactories() ) {
                    // Internal consistency check
                    if ( factory == null ) {
                        Log.warn(this, "SERVICE <" + substitutedOp.getService().toString() + ">: Null item in custom ServiceExecutionRegistry");
                        continue;
                    }

                    svcExec = factory.createExecutor(substitutedOp, substitutedOp, input, execCxt);
                    if ( svcExec != null )
                        break;
                }
            }

            // ---- Execute
            if ( svcExec == null )
                throw new QueryExecException("No SERVICE handler");
            QueryIterator qIter = svcExec.exec();
            qIter = QueryIter.makeTracked(qIter, execCxt);
            // Need to put the outerBinding as parent to every binding of the service call.
            // There should be no variables in common because of the OpSubstitute.substitute
            // return new QueryIterCommonParent(qIter, outerBinding, getExecContext());

            return qIter;

            //PartitionIterator result = new PartitionIterator(opService, serviceVars, qIter, idxVar, bulk, bulkSize, renames);

        } catch (RuntimeException ex) {
            if ( silent ) {
                Log.warn(this, "SERVICE " + NodeFmtLib.strTTL(substitutedOp.getService()) + " : " + ex.getMessage());
                // Return the input
                return QueryIterSingleton.create(input, execCxt);

            }
            throw ex;
        }
	}
}
