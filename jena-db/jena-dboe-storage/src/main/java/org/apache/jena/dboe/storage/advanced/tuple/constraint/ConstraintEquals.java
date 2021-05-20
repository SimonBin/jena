package org.apache.jena.dboe.storage.advanced.tuple.constraint;

import java.util.Objects;

public class ConstraintEquals<C>
	implements Constraint<C>
{
	protected C constraintValue;
	
	public ConstraintEquals(C constraintValue) {
		super();
		this.constraintValue = constraintValue;
	}

	public C getConstraintValue() {
		return constraintValue;
	}
	
	@Override
	public boolean test(C testValue) {
		boolean result = Objects.equals(testValue, constraintValue);
		return result;
	}
}
