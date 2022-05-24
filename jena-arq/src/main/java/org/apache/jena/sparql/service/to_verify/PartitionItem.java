package org.apache.jena.sparql.service.to_verify;

import org.apache.jena.sparql.engine.binding.Binding;

public class PartitionItem implements PartitionElt {
	protected Binding outputBinding;

	public PartitionItem(Binding binding) {
		super();
		this.outputBinding = binding;
	}

	public Binding getOutputBinding() {
		return outputBinding;
	}

	@Override public boolean isItem() { return true; }
	@Override public PartitionItem asItem() { return this; }
}