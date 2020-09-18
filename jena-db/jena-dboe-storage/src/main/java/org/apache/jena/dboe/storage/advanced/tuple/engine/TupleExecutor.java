package org.apache.jena.dboe.storage.advanced.tuple.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessor;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorQuad;
import org.apache.jena.dboe.storage.advanced.tuple.TupleAccessorTriple;
import org.apache.jena.dboe.storage.advanced.tuple.TupleQuery;
import org.apache.jena.dboe.storage.advanced.tuple.TupleQueryImpl;
import org.apache.jena.dboe.storage.advanced.tuple.TupleQuerySupport;
import org.apache.jena.dboe.storage.advanced.tuple.resultset.ResultStreamer;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.util.NodeUtils;

public class TupleExecutor {
    public static <T> Stream<Binding> findTriple(
            boolean distinct,
            T tuple,
            TupleAccessor<T, Node> accessor,
            TupleQuerySupport<Triple, Node> tupler) {
        return find(3, distinct, tuple, accessor, TupleAccessorTriple.INSTANCE, tupler);
    }

    public static <T> Stream<Binding> findQuad(
            boolean distinct,
            T tuple,
            TupleAccessor<T, Node> accessor,
            TupleQuerySupport<Quad, Node> tupler) {
        return find(4, distinct, tuple, accessor, TupleAccessorQuad.INSTANCE, tupler);
    }

    public static <D, T> Stream<Binding> find(
            int dimension,
            boolean distinct,
            T tuple,
            TupleAccessor<T, Node> accessor,
            TupleAccessor<D, Node> domainAccessor,
            TupleQuerySupport<D, Node> tupler) {

        TupleQuery<Node> tupleQuery = new TupleQueryImpl<>(dimension);

        // Initially project nothing (default is to project everything)
        tupleQuery.setProject();
        tupleQuery.setDistinct(true);

        List<Var> projectToVar = new ArrayList<>();

        for (int i = 0; i < dimension; ++i) {
            Node component = accessor.get(tuple, i);
            if (NodeUtils.isNullOrAny(component)) {
                // Nothing to do
            } else if (component.isVariable()) {
                Var var = (Var)component;
                // Blank nodes will not be projected
                if (!Var.isBlankNodeVar(var)) {
                    projectToVar.add(var);
                    tupleQuery.addProject(i);
                }
            } else {
                tupleQuery.setConstraint(i, component);
            }
        }

        if(tupleQuery.getProject().length == 3) {
            tupleQuery.clearProject();
        }

        Var[] projectToVarArr = projectToVar.toArray(new Var[0]);

        Stream<Binding> result;

        ResultStreamer<D, Node, Tuple<Node>> rs = tupler.find(tupleQuery);
        switch (rs.getBackingType()) {
        case DOMAIN:
            result = rs.streamAsDomainObject().map(triple -> {
                BindingHashMap binding = new BindingHashMap();
                binding.add(projectToVarArr[0], domainAccessor.get(triple, 0));
                binding.add(projectToVarArr[1], domainAccessor.get(triple, 1));
                binding.add(projectToVarArr[2], domainAccessor.get(triple, 2));

                return (Binding)binding;
            });
            break;
        case TUPLE:
            result = rs.streamAsTuple().map(tup -> {
                BindingHashMap binding = new BindingHashMap();
                for (int i = 0; i < projectToVarArr.length; ++i) {
                    Var var = projectToVarArr[i];
                    Node val = tup.get(i);
                    binding.add(var, val);
                }
                return (Binding)binding;
            });
            break;
        case COMPONENT:
            Var var = projectToVarArr[0];
            result = rs.streamAsComponent().map(node -> {
                Binding binding = BindingFactory.binding(var , node);
                return binding;
            });
            break;
        default:
            throw new IllegalStateException("Should never come here");
        }

        return result;
    }
}
