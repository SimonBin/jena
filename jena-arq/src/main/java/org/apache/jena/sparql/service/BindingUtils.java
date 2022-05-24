package org.apache.jena.sparql.service;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

public class BindingUtils {

	public static <C extends Collection<Var>> C varsMentioned(C out, Iterator<Binding> it) {
		while (it.hasNext()) {
			Binding b = it.next();
			CollectionUtils.addAll(out, b.vars());
		}
		return out;
	}

	public static Set<Var> varsMentioned(Iterable<Binding> bindings) {
		Set<Var> result = new LinkedHashSet<>();
		return varsMentioned(result, bindings.iterator());
	}

	public static Set<Var> varsMentioned(Binding binding) {
		Set<Var> result = new LinkedHashSet<>();
		binding.vars().forEachRemaining(result::add);
		return result;
	}

}
