package org.apache.jena.sparql.service;

/**
 * A partition element (event) can be either a marker for the start of a partition
 * or an item in a partition.
 *
 * The start marker is holds the parent binding that was used to
 * instantiate the service pattern in preparation for execution.
 * The partition items are the subsequent bindings that were resulted from the execution.
 *
 */
public interface PartitionElt {
	default boolean isStart() { return false; }
	default boolean isItem() { return false; }

	default PartitionStart asStart() { throw new IllegalStateException("Not a start element"); }
	default PartitionItem asItem() { throw new IllegalStateException("Not an item element"); }
}