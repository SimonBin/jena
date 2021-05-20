package org.apache.jena.dboe.storage.advanced.tuple.constraint;

import org.apache.jena.graph.Node;

/**
 * Constraint for an IRI whose string representation starts
 * with a certain sub string.
 * 
 * TODO Constraints such as this one are actually compound conjunctive ones
 *   So we can have multiple constraints on a single component 
 * 
 * (Node = RDF term)
 * Implies NodeConstraintNodeType(NodeComponent.TYPE, NodeType.IRI) &&
 *         NodeConstraintStartsWith(NodeComponent.VALUE, "the://prefix") 
 * 
 * @author Claus Stadler
 *
 */
public class ConstraintIriPrefix
	implements Constraint<Node>
{
	protected String iriPrefix;
	
	public ConstraintIriPrefix(String iriPrefix) {
		super();
		this.iriPrefix = iriPrefix;
	}

	public String getConstraintValue() {
		return iriPrefix;
	}
	
	@Override
	public boolean test(Node testValue) {
		boolean result = testValue.isURI() && testValue.getURI().startsWith(iriPrefix);
		return result;
	}
}
