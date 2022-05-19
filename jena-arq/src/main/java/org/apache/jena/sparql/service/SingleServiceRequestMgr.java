package org.apache.jena.sparql.service;

import java.util.List;
import java.util.Map;

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Node;
import org.apache.jena.query.QueryExecException;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterSingleton;

/**
 * Manager class to help grouping requests (bindings) to the same service
 * and mapping the results to the proper output order
 */
public class SingleServiceRequestMgr {
	protected OpServiceInfo serviceInfo;
	protected Node serviceNode;

	protected List<PartitionRequest> requests;
	protected Map<Long, Integer> outputIdToRequestIdx;

	void add(PartitionRequest request) {
		requests.add(request);
	}

	public  PartitionIterator execPartition(boolean silent, ExecutionContext execCxt, ServiceExecutorRegistry registry,
			int bulkSize, Binding[] bulk, Binding outerBinding, Var idxVar,
			OpService substitutedOp) {

        ServiceExecution svcExec = null;
		try {
            // ---- Find handler
            if ( registry != null ) {
                for ( ServiceExecutorFactory factory : registry.getFactories() ) {
                    // Internal consistency check
                    if ( factory == null ) {
                        Log.warn(this, "SERVICE <" + serviceInfo.getOpService() opService.getService().toString() + ">: Null item in custom ServiceExecutionRegistry");
                        continue;
                    }

                    svcExec = factory.createExecutor(substitutedOp, opService, outerBinding, execCxt);
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


            PartitionIterator result = new PartitionIterator(serviceInfo.getOpService(), serviceInfo.getServiceVars(), qIter, idxVar, bulk, bulkSize, serviceInfo.getRenames());

    		return result;

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
