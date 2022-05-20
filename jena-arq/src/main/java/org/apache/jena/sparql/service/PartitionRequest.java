package org.apache.jena.sparql.service;

import org.apache.jena.sparql.engine.binding.Binding;

public class PartitionRequest {
	protected Binding partition;
	protected long outputId;
	protected long offset;
	protected long limit;

	public PartitionRequest(
			Binding partition,
			long outputId,
			long offset,
			long limit) {
		super();
		this.partition = partition;
		this.outputId = outputId;
		this.offset = offset;
		this.limit = limit;
	}

	public Binding getPartition() {
		return partition;
	}

	public long getOutputId() {
		return outputId;
	}

	public long getOffset() {
		return offset;
	}

	public long getLimit() {
		return limit;
	}
}
