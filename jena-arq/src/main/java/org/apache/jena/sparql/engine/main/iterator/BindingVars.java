package org.apache.jena.sparql.engine.main.iterator;

import java.util.Collection;
import java.util.Iterator;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

public class BindingVars {
	public static <C extends Collection<Var>> C addAll(C acc, Binding binding) {
		Iterator<Var> it = binding.vars();
		while (it.hasNext()) {
			Var v = it.next();
			acc.add(v);
		}
		return acc;
	}
}
