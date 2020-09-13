package org.apache.jena.dboe.storage.advanced.tuple.hierarchical;

import java.util.Map;
import java.util.stream.Stream;



//public class TupleIndexNodeFromIncomingMaps<
//        ThisKeyType,
//        ThisValueType,
//        NextValueType,
//        IncomingValueType extends Map<ThisKeyType, NextValueType>
//    >
//    implements TupleIndexNode<ThisValueType, ThisKeyType, NextValueType, IncomingValueType>
//{
////
////	public static TupleIndexNode<> create(ThisKeyType constraint, TupleIndexNode<?, ?, IncomingValueType, ?> parent) {
////
////	}
////
//
//    public TupleIndexNodeFromIncomingMaps(ThisKeyType constraint, TupleIndexNode<?, ?, IncomingValueType, ?> parent) {
//        super();
//        this.constraint = constraint;
//        this.parent = parent;
//    }
//
////    abstract ThisKeyType getConstraint();
////
////    abstract TupleIndexNode<?, ?, IncomingValueType, ?> getParent();
//
////    protected ThisKeyType constraint;
//    protected TupleIndexNode<?, ?, IncomingValueType, ?> parent;
//
////    public ThisKeyType getConstraint() {
////        return constraint;
////    }
//
//    public TupleIndexNode<?, ?, IncomingValueType, ?> getParent() {
//        return parent;
//    }
//
//
////    public void getOrCreate(ThisKeyType key) {
////
////    }
//
//    @Override
//    public Stream<NextValueType> streamContributions(
//            IncomingValueType incoming,
//            ThisKeyType constraint
//            ) {
//        Stream<NextValueType> result;
//        // TODO Replace with range?
//        //ThisKeyType key = getConstraint();
//        if (key == null) {
//            result = incoming.values().stream();
//        } else {
//            NextValueType v = incoming.get(key);
//            result = v == null
//                    ? Stream.empty()
//                    : Stream.of(v);
//        }
//
//        return result;
//    }
//
//    @Override
//    public Stream<NextValueType> streamParent() {
//        Stream<IncomingValueType> incomingStream = getParent().streamParent();
//
//        Stream<NextValueType> result = incomingStream.flatMap(incoming -> streamContributions(incoming));
//        return result;
//    }
//}
