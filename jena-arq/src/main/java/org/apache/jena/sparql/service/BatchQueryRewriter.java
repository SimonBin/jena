package org.apache.jena.sparql.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.query.Query;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingProject;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementData;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementSubQuery;

/**
 * Rewriter for instantiating a query such that a list of initial bindings are injected.
 * In general, there are several rewriting strategies for that purpose and their applicability
 * depends on the operations used in of the query:
 *
 * <ul>
 * <li>Union/Substitution strategy: The simplest and most verbose strategy is to create a union query where
 * for every input binding a union member is obtained by substituting the original
 * query with it</li>
 * <li>Join strategy: The input bindings are collected into a VALUES block and placed on the left hand size
 * of a join with an adjusted version of the original query.</li>
 * <li>Filter strategy: Input bindings are turned into a disjunctive filter expression (not yet implemented)</li>
 * </ul>
 *
 */
public class BatchQueryRewriter {
	protected Query rawQuery;
	protected Op rawOp;
	protected Map<Var, Var> renames;

	protected Var idxVar;
	protected Set<Var> serviceVars;

	public BatchQueryRewriter(Query rawQuery, Op rawOp, Map<Var, Var> renames, Var idxVar, Set<Var> serviceVars) {
		super();
		this.rawQuery = rawQuery;
		this.rawOp = rawOp;
		this.renames = renames;
		this.idxVar = idxVar;
		this.serviceVars = serviceVars;
	}

	public BatchQueryRewriteResult rewrite(
    		Binding[] bulk,
    		int bulkLen,
    		Set<Var> seenVars) {

		BatchQueryRewriteResult result = rawQuery.hasLimit() || rawQuery.hasOffset()
			? rewriteAsUnion(bulk, bulkLen, seenVars)
			: rewriteAsJoin(bulk, bulkLen, seenVars);

		return result;
	}

    public BatchQueryRewriteResult rewriteAsUnion(
    		Binding[] bulk,
    		int bulkLen,
    		Set<Var> seenVars) {

    	Op newOp = null;
    	for (int i = bulkLen - 1; i >= 0; --i) {
    		Binding b = bulk[i];

    		Op op = QC.substitute(rawOp, b);
    		if (bulkLen > 1) {
    			op = OpExtend.create(op, idxVar, NodeValue.makeInteger(i));
    		}

    		newOp = newOp == null ? op : OpUnion.create(op, newOp);
    	}


    	Query q = OpAsQuery.asQuery(newOp);
        System.err.println(q);


        // Op newSubOp = Algebra.compile(q);
        // Op newSubOp = OpJoin.create(OpTable.create(table), subOp);

        return new BatchQueryRewriteResult(newOp, renames);

    }


    public BatchQueryRewriteResult rewriteAsJoin(
    		Binding[] bulk,
    		int bulkLen,
    		Set<Var> seenVars) {
        Query q;


    	Set<Var> joinVars = new LinkedHashSet<>(serviceVars);
    	joinVars.retainAll(seenVars);


    	// Project the bindings to those variables that are also visible
    	// in the service cause
    	for (int i = 0; i < bulkLen; ++i) {
    		bulk[i] = BindingFactory.binding(
    				new BindingProject(joinVars, bulk[i]),
    				idxVar, NodeValue.makeInteger(i).asNode());
    	}
        List<Binding> bulkList = Arrays.asList(bulk).subList(0, bulkLen);

        boolean wrapAsSubQuery = needsWrappingByFeatures(rawQuery);
        if (wrapAsSubQuery) {
        	q = new Query();
        	q.setQuerySelectType();
        	q.setQueryPattern(new ElementSubQuery(rawQuery));
        	q.getProjectVars().addAll(rawQuery.getProjectVars());
        	if (rawQuery.hasOrderBy()) {
        		q.getOrderBy().addAll(rawQuery.getOrderBy());
            	rawQuery.getOrderBy().clear();
        	}
        } else {
        	q = rawQuery;
        }

        boolean injectIdx = true;

        if (injectIdx) {
        	SortCondition sc = new SortCondition(new ExprVar(idxVar), Query.ORDER_ASCENDING);
        	if (q.hasOrderBy()) {
        		q.getOrderBy().add(0, sc);
        	} else {
        		q.addOrderBy(sc);
        	}
        }

        q.resetResultVars();
        q.setQueryResultStar(false);
        q.getProjectVars().removeAll(joinVars);
        q.getProjectVars().add(0, idxVar);
        q.resetResultVars();

        List<Var> remoteVars = new ArrayList<>(1 + joinVars.size());
        remoteVars.add(idxVar);
        remoteVars.addAll(joinVars);
        ElementData dataBlock = new ElementData(remoteVars, bulkList);
        Element before = q.getQueryPattern();
        ElementGroup after = new ElementGroup();
    	after.addElement(dataBlock);
        if (before instanceof ElementGroup) {
        	((ElementGroup)before).getElements().forEach(after::addElement);
        } else {
        	after.addElement(before);
        }
        q.setQueryPattern(after);

        // LOG.
        System.err.println(q);

        Op newSubOp = Algebra.compile(q);
        // Op newSubOp = OpJoin.create(OpTable.create(table), subOp);

        return new BatchQueryRewriteResult(newSubOp, renames);
    }



    /**
     * Returns true if the query uses features that prevents it from being
     * represented as a pair of graph pattern + projection
     *
     * @param query
     * @return
     */
    public static boolean needsWrappingByFeatures(Query query) {
        return needsWrappingByFeatures(query, true) || !query.getProject().getExprs().isEmpty();
    }


    /**
     * Similar to {@link #needsWrapping(Query)} but includes a flag
     * whether to include slice information (limit / offset).
     *
     * @param query
     * @return
     */
    public static boolean needsWrappingByFeatures(Query query, boolean includeSlice) {
        boolean result
             = query.hasGroupBy()
            || query.hasAggregators()
            || query.hasHaving()
            || query.hasValues();

        if (includeSlice) {
            result = result
                || query.hasLimit()
                || query.hasOffset()
                ;
        }

        // Order is ignored

        return result;
    }


    public static class BatchQueryRewriteResult {
    	protected Op op;
    	protected Map<Var, Var> renames;

    	public BatchQueryRewriteResult(Op op, Map<Var, Var> renames) {
			super();
			this.op = op;
			this.renames = renames;
		}

    	public Op getOp() {
			return op;
		}

    	public Map<Var, Var> getRenames() {
			return renames;
		}
    }


}