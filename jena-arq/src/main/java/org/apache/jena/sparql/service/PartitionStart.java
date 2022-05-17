package org.apache.jena.sparql.service;

import java.util.Set;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

public class PartitionStart implements PartitionElt {
	protected Op op;
	protected Op substitutedOp;
	protected Set<Var> vars;
	protected Binding parentBinding;

	public PartitionStart(Op op, Op substitutedOp, Set<Var> vars, Binding parentBinding) {
		super();
		this.op = op;
		this.substitutedOp = substitutedOp;
		this.vars = vars;
		this.parentBinding = parentBinding;
	}

	@Override public boolean isStart() { return true; }
	@Override public PartitionStart asStart() { return this; }
}