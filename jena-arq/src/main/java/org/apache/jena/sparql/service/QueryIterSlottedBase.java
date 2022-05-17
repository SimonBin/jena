package org.apache.jena.sparql.service;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.io.PrintUtils;
import org.apache.jena.atlas.iterator.IteratorSlotted;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.util.QueryOutputUtils;

/**
 * QueryIterator implementation based on IteratorSlotted.
 * Its purpose is to ease wrapping a non-QueryIterator as one based
 * on a {@link #moveToNext()} method.
 */
public abstract class QueryIterSlottedBase<T>
	extends IteratorSlotted<Binding>
	implements QueryIterator
{
	@Override
	public Binding nextBinding() {
		Binding result = next();
		return result;
	}

	@Override
	protected boolean hasMore() {
		return true;
	}

    @Override
    public String toString(PrefixMapping pmap)
    { return QueryOutputUtils.toString(this, pmap) ; }

    // final stops it being overridden and missing the output() route.
    @Override
    public final String toString()
    { return PrintUtils.toString(this) ; }

    /** Normally overridden for better information */
    @Override
    public void output(IndentedWriter out)
    {
        out.print(Plan.startMarker) ;
        out.print(Lib.className(this)) ;
        out.print(Plan.finishMarker) ;
    }

    @Override
	public void cancel() {
    	close();
	}

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
    	output(out);
//	        out.println(Lib.className(this) + "/" + Lib.className(iterator));
//	        out.incIndent();
//	        // iterator.output(out, sCxt);
//	        out.decIndent();
//	        // out.println(Utils.className(this)+"/"+Utils.className(iterator)) ;
    }
}
