# Service Enhancer Plugin

This plugin extends the functionality of the SERVICE clause with:

- Bulk requests
- Correlated joins also known as lateral joins
- Caching

As a fundamental principle, a request making use of cache and bulk should return the exact same result as if
those settings were omitted. As a consequence runtime result set size recognization (RRR) is employed to reveal hidden
result set limits and ensure that always only the appropriate amount of data is yeld from the caches.

## Example
The following query executes as a single remote request to Wikidata

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT ?s ?l {
  # The ids below correspond in order to: Apache Jena, Semantic Web, RDF, SPARQL, Andy Seaborne
  VALUES ?s { wd:Q1686799 wd:Q54837 wd:Q54872 wd:Q54871 wd:Q108379795 }
 
  SERVICE <cache:loop:bulk+5:https://query.wikidata.org/sparql> {
    SELECT ?l {
      ?s rdfs:label ?l
      FILTER(langMatches(lang(?l), 'en'))
    } ORDER BY ?l LIMIT 1
  }
}
```


<details>
  <summary markdown="span">Rewritten Query</summary>

```sparql
SELECT  *
WHERE
  {   {   { { SELECT  *
              WHERE
                { { SELECT  ?l
                    WHERE
                      { <http://www.wikidata.org/entity/Q1686799>
                                  <http://www.w3.org/2000/01/rdf-schema#label>  ?l
                        FILTER langMatches(lang(?l), "en")
                      }
                  }
                  BIND(0 AS ?__idx__)
                }
              LIMIT   1
            }
          }
        UNION
          {   { { SELECT  *
                  WHERE
                    { { SELECT  ?l
                        WHERE
                          { <http://www.wikidata.org/entity/Q54837>
                                      <http://www.w3.org/2000/01/rdf-schema#label>  ?l
                            FILTER langMatches(lang(?l), "en")
                          }
                      }
                      BIND(1 AS ?__idx__)
                    }
                  LIMIT   1
                }
              }
            UNION
              {   { { SELECT  *
                      WHERE
                        { { SELECT  ?l
                            WHERE
                              { <http://www.wikidata.org/entity/Q54872>
                                          <http://www.w3.org/2000/01/rdf-schema#label>  ?l
                                FILTER langMatches(lang(?l), "en")
                              }
                          }
                          BIND(2 AS ?__idx__)
                        }
                      LIMIT   1
                    }
                  }
                UNION
                  {   { { SELECT  *
                          WHERE
                            { { SELECT  ?l
                                WHERE
                                  { <http://www.wikidata.org/entity/Q54871>
                                              <http://www.w3.org/2000/01/rdf-schema#label>  ?l
                                    FILTER langMatches(lang(?l), "en")
                                  }
                              }
                              BIND(3 AS ?__idx__)
                            }
                          LIMIT   1
                        }
                      }
                    UNION
                      { { SELECT  *
                          WHERE
                            { { SELECT  ?l
                                WHERE
                                  { <http://www.wikidata.org/entity/Q108379795>
                                              <http://www.w3.org/2000/01/rdf-schema#label>  ?l
                                    FILTER langMatches(lang(?l), "en")
                                  }
                              }
                              BIND(4 AS ?__idx__)
                            }
                          LIMIT   1
                        }
                      }
                  }
              }
          }
      }
    UNION
      { BIND(1000000000 AS ?__idx__) }
  }
ORDER BY ASC(?__idx__) ?l
```

</details>


- IRI for 'self' `urn-xarq:self`


```sparql
PREFIX sepf: <java:org.apache.jena.sparql.service.enhancer.pfunction.>
SELECT * WHERE {
  ?id sepf:cacheList (?service ?query ?binding ?start ?end)
}
```

```
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| id | service                           | query                                                                                              | binding             | start | end |
===================================================================================================================================================================================
| 2  | "urn:x-arq:self@dataset813601419" | "SELECT  (<urn:default> AS ?g) ?p (count(*) AS ?c)\nWHERE\n  { ?s  a  ?o }\nGROUP BY ?p\n"         | "( ?p = rdf:type )" | 0     | 1   |
| 3  | "urn:x-arq:self@dataset813601419" | "SELECT  ?g ?p (count(*) AS ?c)\nWHERE\n  { GRAPH ?g\n      { ?s  a  ?o }\n  }\nGROUP BY ?g ?p\n"  | "( ?p = rdf:type )" | 0     | 0   |
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
```



```sparql
PREFIX sefn: <java:org.apache.jena.sparql.service.enhancer.function.>
SELECT (sefn:cacheInvalidate() AS ?count) {
}
```

```
PREFIX sepf: <java:org.apache.jena.sparql.service.enhancer.pfunction.>
PREFIX sefn: <java:org.apache.jena.sparql.service.enhancer.function.>

SELECT SUM(sefn:cacheInvalidate(?id) AS ?count) {
  ?id sepf:cacheList ()
}
```


## Assembler

The `ja:DatasetServiceEnhancer` assembler can be used to enable the SE plugin on a dataset.
Note, that the assembler returns the base dataset itself however with re-configured context.


```ttl
PREFIX ja: <http://jena.hpl.hp.com/2005/11/Assembler#> .
<#root>
  a ja:DatasetServiceEnhancer ;
  ja:baseDataset <#base> ;
  ja:selfId <https://my.dataset.id/> ; # Defaults to the value of ja:baseDataset
  .

<#base> a ja:MemoryDataset .
```

The value of `ja:selfId` is used to look up caches when referring to the active dataset using `SERVICE <urn:x-arq:self> {}`.


## How it works

### Terminology

* **Lhs & Rhs** Left/right hand side is a common convention to refer to the arguments of operations with two arguments.
* **Scope rename** SPARQL evaluation has a notion of scoping which determines whether a variable will be part of the solution bindings created from a graph pattern [as defined here](https://www.w3.org/TR/sparql11-query/#variableScope). Jena provides `TransformScopeRename` which renames variables such as their names are globally. Jena's scope renaming prepends `/` characters before the original variable name so `?x` may become `?/x` or `?//x`.
* **Substitution** When evaluating the lhs of a join then the scope renaming enables that for each obtained binding all variables on the rhs can be substituted with the corresponding values of that binding.
* **Base name** The base name of a variable is it's name without scoping. For example the variables `?x`, `?/x` and `?//x` all have the base name `x`.

### Overview
In order to make `loop:` work the following machinery is in place:

The algebra transformation implemented by `TransformSE_JoinStrategy` needs to run bothe **before** and **after** the default algebra optimization.
The reason is that is does two things:
* It converts every OpJoin instance with a `loop:`  on the right hand side into a `OpSequence`.
* Any **mentioned** variable on the rhs whose base name matches the base name of a **visible** variable on the lhs gets substituted by the lhs variable.


The snippet below shows how to apply the transformation programmatically.
```java
String queryStr = "copy from the examples below";
Transform loopTransform = new TransformSE_JoinStrategy();
Op op0 = Algebra.compile(QueryFactory.create(queryStr));
Op op1 = Transformer.transform(loopTransform, op0);
Op op2 = Optimize.stdOptimizationFactory.create(ARQ.getContext()).rewrite(op1);
Op op3 = Transformer.transform(loopTransform, op2);
System.out.println(op3);
```

### Standard Join

Consider the following example.
```sparql
SELECT ?p ?c {
  BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> AS ?p)
  { SELECT (COUNT(*) AS ?c) { ?s ?p ?o } }
}
```

Note that the `?p` on the right hand side becomes scoped as `?/p`. Consequently, lhs' `?p`  and rhs' `?/p` are considered different variables.
```
(project (?p ?c)
  (join
    (extend ((?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>))
      (table unit))
    (project (?c)
      (extend ((?c ?/.0))
        (group () ((?/.0 (count)))
          (bgp (triple ?/s ?/p ?/o)))))))
```


### Looping

The two effects of the `loop:` transform are shown below. First, a `sequence` is enforced. And second, the scope of `?p` is now the same on the lhs and rhs.
```sparql
SELECT ?p ?c {
  BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> AS ?p)
  SERVICE <loop:> { SELECT (COUNT(*) AS ?c) { ?s ?p ?o } }
}
```

```
(project (?p ?c)
  (sequence
    (extend ((?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>))
      (table unit))
    (service <loop:>
      (project (?c)
        (extend ((?c ?/.0))
          (group () ((?/.0 (count)))
            (bgp (triple ?/s ?p ?/o))))))))
```

Upon evaluation, for each binding of the lhs the `?p` on the rhs is now substituted thus giving the count for the specific property.


