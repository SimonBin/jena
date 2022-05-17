package org.apache.jena.sparql.service;

import org.apache.jena.sparql.engine.binding.Binding;

public class PartitionItem implements PartitionElt {
	protected Binding binding;

	public PartitionItem(Binding binding) {
		super();
		this.binding = binding;
	}

	public Binding getBinding() {
		return binding;
	}

	@Override public boolean isItem() { return true; }
	@Override public PartitionItem asItem() { return this; }
}