package org.apache.jena.sparql.service;

import java.util.Objects;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.binding.Binding;

public class ServiceCacheKey {

	// Note: The reason why serviceNode and Op are note combined as a OpService
	// is because for a cache key, the serviceNode has to be concrete (i.e. substitution applied), whereas the
	// service's subOp (here 'op') has to be as a given (without substitution).

	protected Node serviceNode;
	protected Op op;
	// protected Set<Var> joinVars;
	protected Binding binding;

	public ServiceCacheKey(Node serviceNode, Op op, Binding binding) {
		super();
		this.serviceNode = serviceNode;
		this.op = op;
		this.binding = binding;
	}

	public Node getServiceNode() {
		return serviceNode;
	}

	public Op getOp() {
		return op;
	}

	public Binding getBinding() {
		return binding;
	}

	@Override
	public int hashCode() {
		return Objects.hash(serviceNode, op, binding);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ServiceCacheKey other = (ServiceCacheKey) obj;
		return Objects.equals(binding, other.binding) && Objects.equals(op, other.op)
				&& Objects.equals(serviceNode, other.serviceNode);
	}

	@Override
	public String toString() {
		return "ServiceCacheKey [serviceNode=" + serviceNode + ", op=" + op + ", binding=" + binding + "]";
	}
}