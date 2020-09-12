package org.apache.jena.dboe.storage.tuple;

import java.util.function.Function;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;

public class TupleOps {
    public static <DomainType, ComponentType> Function<DomainType, Tuple<ComponentType>>
    createProjector(int[] project, TupleAccessor<? super DomainType, ? extends ComponentType> accessor) {
        Function<DomainType, Tuple<ComponentType>> result;

        int len = accessor.getRank();
        switch(len) {
        case 1: result = domain -> TupleFactory.create1(
                accessor.get(domain, project[0])); break;
        case 2: result = domain -> TupleFactory.create2(
                accessor.get(domain, project[0]),
                accessor.get(domain, project[1])); break;
        case 3: result = domain -> TupleFactory.create3(
                accessor.get(domain, project[0]),
                accessor.get(domain, project[1]),
                accessor.get(domain, project[2])); break;
        case 4: result = domain -> TupleFactory.create4(
                accessor.get(domain, project[0]),
                accessor.get(domain, project[1]),
                accessor.get(domain, project[2]),
                accessor.get(domain, project[3])); break;
        default: result = domain -> project(project, domain, accessor); break;
        }

        return result;
    }

    public static <DomainType, ComponentType> Tuple<ComponentType> project(
            int[] proj,
            DomainType domainObject,
            TupleAccessor<? super DomainType, ? extends ComponentType> accessor) {
        @SuppressWarnings("unchecked")
        ComponentType[] tuple = (ComponentType[])new Object[proj.length];
        for(int i = 0; i < proj.length; ++i) {
            tuple[i] = accessor.get(domainObject, proj[i]);
        }
        return TupleFactory.create(tuple);
    }
}
