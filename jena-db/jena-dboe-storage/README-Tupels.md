## Abstract

This is a summary of the components and their designs of the tuple-based storage system.

## Motivation

The original goal was to contribute insert-order aware/preserving graph / dataset implementations.
The basic idea was to use a nested map structure such as `Map<Node, Map<Node, Map<Node, Triple>>>` in conjunction with LinkedHashMap implementations.


Insert order *preservation*: Enumerate triples/quads in exactly the order in which they were inserted.
Insert order *sensitivity* is a weaker form: Enumerate triples/quad in a way that is based on the insert order.
For example, the use of LinkedHashMaps does not preserve insert order, but the triples/quads are grouped based on the RDF terms encountered first.
```
1| :s1 :p :o
2| :s2 :p :o
3| :s1 :x :y
```

```
1| :s1 :p :o
2| :s1 :x :y
3| :s2 :p :o
```


## Tuple Abstraction

Triples and Quads are tuple-like objects; i.e. they are entities having three or four components accessible via indices of type integer.
Tuples have a fixed dimension.
There are many other tuple like objects, such as Lists and arrays.
As unrelated tuple-like entities do not implement a common 'Tuple' interface we use an accessor pattern to access component values of tuple-like object.
In the domain of RDF, triple and quad are well defined tuple-like objects. We refer to such tuple-like objects of significance as 'domain tuples'.

The following accessors are introduced:

* `TupleAccessorCore` is a simple functional interface that enables access to a tuple-like object via an integer index. The interface can be used for lambdas and
and allows for method references to e.g. `List::get`.
```
@FunctionalInterface
public interface TupleAccessorCore<TupleLike, ComponentType>
{
    ComponentType get(TupleLike tupleLike, int componentIdx);
}
```

* `TupleAcessor` (w/o `Core`) is a subclass of TupleAccessor that is bound to a certain domain (i.e. for example Triple or Quad).
It provides the method `restore` that should yieled a domain tuple from another tuple-like object. This means that e.g. a triple can be created from a `List<Node>`
using `Triple t = tripleAccessor.restore(listOfNodes, List::get);


### Transformations
TupleAccessor can be easily adapted to apply transformations before returning component values. For example, a tuple accessor implementation may transform Node.ANY to null.


:warning: The relation between null and ANY may not be stable yet: Currently my view is that the test whether a specific value for a component
bears the meaning of the placeholder belongs to the
tupleAccessor - `boolean verdict = tupleAccessor.isAny(componentValue)`


## StorageNode System

The storage node system is a flexible system to store tuple-like objects and index them by their components.
It serves the following purposes:
* Specification of nested data structures
* Mapping of tuple components to these nested structures
* Instantiation of store objects
* Insertion and removal of tuples into stores


A store is a concrete data structure, such as a Set<> or Map<>.
The StorageNode can allocate nested Maps as the 


`Map<Node, Map<Node, Map<Node, Triple>>>`


```
    /**
     * Create a conventional storage with SPO, OPS and POS index
     *
     * @returns
     */
    public static StorageNodeMutable<Triple, Node, ?> createConventionalStorage() {
        StorageNodeMutable<Triple, Node, ?> storage =
                alt3(
                    // spo
                    innerMap(0, HashMap::new,
                        innerMap(1, HashMap::new,
                            leafMap(2, HashMap::new, TupleAccessorTriple.INSTANCE)))
                    ,
                    // ops
                    innerMap(2, HashMap::new,
                        innerMap(1, HashMap::new,
                            leafMap(0, HashMap::new, TupleAccessorTriple.INSTANCE)))
                    ,
                    // pos
                    innerMap(1, HashMap::new,
                        innerMap(2, HashMap::new,
                            leafMap(0, HashMap::new, TupleAccessorTriple.INSTANCE)))
                );

        return storage;
    }
```




### TupleQueryAnalyzer




