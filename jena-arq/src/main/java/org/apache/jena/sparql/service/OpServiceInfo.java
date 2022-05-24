package org.apache.jena.sparql.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.Rename;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;

/**
 * Class holding information derived from an OpService, such as the mentioned variables
 * and the syntactic representation.
 */
public class OpServiceInfo {
    protected OpService opService; // Original opService
    protected Node serviceNode;
    protected Var serviceVar;

    protected Query rawQuery; // Query for opService.getSubOp() without slice
    protected Op rawQueryOp; // Algebra.compile(rawQuery)

    protected long limit;
    protected long offset;

    protected Map<Var, Var> renames = new HashMap<>();

    protected Set<Var> serviceVars;


	public OpServiceInfo(OpService opService) {
        this.opService = opService ;

        this.serviceNode = opService.getService();
        this.serviceVar = serviceNode.isVariable() ? (Var)serviceNode: null;

        // Get the variables used in the service clause (excluding the possible one for the service iri)
        Op subOp = opService.getSubOp();

        // Handling of a null supOp - can that happen?
        this.serviceVars = subOp == null ? Collections.emptySet() : new LinkedHashSet<>(
        		OpVars.visibleVars(subOp)
        		// ISSUE For "BIND(?in AS ?out)" we want ?in to appear as a service / substitution variable
        		// Workaround is a dummy union: SERVICE <> { ... UNION { ?s <urn:dummy> ?s} }
        		//OpVars.mentionedVars(subOp)

        	);


        subOp = Rename.reverseVarRename(subOp, true);

        this.rawQuery = OpAsQuery.asQuery(subOp);
        // this.renames = new HashMap<>();

        VarExprList vel = rawQuery.getProject();
        VarExprList newVel = new VarExprList();

        int allocId = 0;
        for (Var var : vel.getVars()) {
        	Expr expr = vel.getExpr(var);
        	if (Var.isAllocVar(var)) {
        		Var tmp = Var.alloc("__v" + (++allocId) + "__");
        		renames.put(tmp, var);
        		var = tmp;
        	}
        	newVel.add(var, expr);
        }
        vel.clear();
        vel.addAll(newVel);

        this.limit = rawQuery.getLimit();
        this.offset = rawQuery.getOffset();

        rawQuery.setLimit(Query.NOLIMIT);
        rawQuery.setOffset(Query.NOLIMIT);

        rawQueryOp = Algebra.compile(rawQuery);
	}

	public OpService getOpService() {
		return opService;
	}

	public Node getServiceNode() {
		return serviceNode;
	}

	public Node getSubstServiceNode(Binding binding) {
		Node result = serviceVar == null ? serviceNode : binding.get(serviceVar);
		return result;
	}


	public Var getServiceVar() {
		return serviceVar;
	}

	public Set<Var> getServiceVars() {
		return serviceVars;
	}

	public Query getRawQuery() {
		return rawQuery;
	}

	public Op getRawQueryOp() {
		return rawQueryOp;
	}

	public Map<Var, Var> getRenames() {
		return renames;
	}

	public long getLimit() {
		return limit;
	}

	public long getOffset() {
		return offset;
	}
}
