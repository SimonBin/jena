package org.apache.jena.sparql.service;

import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

public class PartitionStart implements PartitionElt {
	protected Op op;
	protected Op substitutedOp;
	protected Set<Var> vars;
	protected Node serviceNode;
	protected Binding inputBinding;

	public PartitionStart(Node serviceNode, Op op, Op substitutedOp, Set<Var> vars, Binding inputBinding) {
		super();
		this.op = op;
		this.serviceNode = serviceNode;
		this.substitutedOp = substitutedOp;
		this.vars = vars;
		this.inputBinding = inputBinding;
	}

	public Node getServiceNode() {
		return serviceNode;
	}

	public Binding getInputBinding() {
		return inputBinding;
	}


	@Override public boolean isStart() { return true; }
	@Override public PartitionStart asStart() { return this; }
}