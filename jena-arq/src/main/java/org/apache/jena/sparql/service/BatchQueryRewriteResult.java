package org.apache.jena.sparql.service;

import java.util.Map;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;


/**
 * Rewrite result of a bulk service request. The 'renames' mapping
 * may turn publicized variables back to internal/anonymous ones.
 * For instance, running <pre>{@code SERVICE <foo> { { SELECT COUNT(*) {...} } }}</pre>
 * will allocate an internal variable for count.
 */
public class BatchQueryRewriteResult {
	protected Op op;
	protected Map<Var, Var> renames;

	public BatchQueryRewriteResult(Op op, Map<Var, Var> renames) { //  Set<Var> joinVars
		super();
		this.op = op;
		this.renames = renames;
	}

	public Op getOp() {
		return op;
	}

	public Map<Var, Var> getRenames() {
		return renames;
	}

	@Override
	public String toString() {
		return "BatchQueryRewriteResult [op=" + op + ", renames=" + renames + "]";
	}
}