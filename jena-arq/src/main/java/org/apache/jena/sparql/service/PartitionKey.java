package org.apache.jena.sparql.service;

import java.util.Objects;

import org.apache.jena.ext.com.google.common.collect.ComparisonChain;

public class PartitionKey
	implements Comparable<PartitionKey>
{
	protected long inputId;
	protected long rangeId;

	public PartitionKey(long inputId, long rangeId) {
		super();
		this.inputId = inputId;
		this.rangeId = rangeId;
	}

	public long getInputId() {
		return inputId;
	}

	public long getRangeId() {
		return rangeId;
	}

	@Override
	public int compareTo(PartitionKey o) {
		int result = ComparisonChain.start()
				.compare(this.getInputId(), o.getInputId())
				.compare(this.getRangeId(), o.getRangeId())
				.result();
		return result;
	}

	@Override
	public String toString() {
		return "PartitionKey [inputId=" + inputId + ", rangeId=" + rangeId + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(inputId, rangeId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PartitionKey other = (PartitionKey) obj;
		return inputId == other.inputId && rangeId == other.rangeId;
	}
}
