package org.apache.jena.sparql.service.to_verify;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.jena.atlas.iterator.IteratorSlotted;
import org.apache.jena.graph.Node;
import org.apache.jena.query.QueryExecException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.expr.ExprEvalException;

public class PartitionIterator
	extends IteratorSlotted<PartitionElt>
{
	// protected ServiceInfo serviceInfo;

	protected QueryIterator qIter;
	protected Var idxVar;
	protected Binding[] bulk;
	protected int bulkSize;
	 protected Map<Var, Var> renames;

	protected PartitionElt pendingEvt = null;
	protected int currentIdx = -1;

	 protected Op originalOp;
	 protected Set<Var> originalOpVars;

    public PartitionIterator(
    		Op originalOp,
    		Set<Var> originalOpVars,
    		QueryIterator qIter,
    		Var idxVar,
    		Binding[] bulk,
    		int bulkSize,
    		Map<Var, Var> renames
    ) {
		super();
		this.originalOp = originalOp;
		this.originalOpVars = originalOpVars;
		this.qIter = qIter;
		this.idxVar = idxVar;
		this.bulk = bulk;
		this.bulkSize = bulkSize;
		this.renames = renames;
	}

    public long getNextOutputId() {
    	return currentIdx;
    }

	@Override
	protected PartitionElt moveToNext() {
		PartitionElt result;

		if (pendingEvt != null) {
			result = pendingEvt;
			pendingEvt = null;
		} else if (qIter.hasNext()) {

			Binding rawChild = qIter.next();// super.moveToNextBinding();
			Binding child;

			if (renames.isEmpty()) {
				child = rawChild;
			} else {
				BindingBuilder bb = BindingFactory.builder();
				Iterator<Var> it = rawChild.vars();
				while (it.hasNext()) {
					Var before = it.next();
					Node node = rawChild.get(before);

					Var after = renames.getOrDefault(before, before);
					bb.add(after, node);
				}
				child = bb.build();
			}


			int idx;
			if (bulkSize > 1) {
				Node idxNode = rawChild.get(idxVar);
				Object obj = idxNode.getLiteralValue();
				if (!(obj instanceof Number)) {
					throw new ExprEvalException("Index was not returned as a number");
				}
				idx = ((Number)obj).intValue();

				if (idx < 0 || idx > bulkSize) {
					throw new QueryExecException("Returned index out of range");
				}
			} else {
				idx = 0;
			}

			Binding parent = bulk[idx];

			if (currentIdx < 0) {
				currentIdx = idx;
			}

			PartitionItem item = new PartitionItem(BindingFactory.builder(parent).addAll(child).build());

			if (currentIdx != idx) {
				Op substitutedOp = QC.substitute(originalOp, parent);

				result = new PartitionStart(null, originalOp, substitutedOp, originalOpVars, parent);
				pendingEvt = item;
			} else {
				result = item;
			}
		} else {
			result = null;
		}

		return result;
	}

	@Override
	protected boolean hasMore() {
		return true;
	}
}