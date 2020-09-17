/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.apache.jena.dboe.storage.advanced.quad;

import java.util.stream.Stream;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 * A diff based QuadTable similar to {@link org.apache.jena.graph.compose.Delta}
 *
 * @author Claus Stadler 11/09/2020
 *
 */
public class QuadTableCoreDelta
    implements QuadTableCore
{
    protected QuadTableCore master;
    protected QuadTableCore additions;
    protected QuadTableCore deletions;

    public void clearDiff() {
        additions.clear();
        deletions.clear();
    }

    public void applyDiff() {
        applyDiff(master, additions, deletions);
        clearDiff();
    }

    public static void applyDiff(QuadTableCore target, QuadTableCore additions, QuadTableCore deletions) {
        deletions.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY).forEach(target::delete);
        additions.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY).forEach(target::add);
    }

    public QuadTableCoreDelta(QuadTableCore master, QuadTableCore additions, QuadTableCore deletions) {
        this.master = master;
        this.additions = additions;
        this.deletions = deletions;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Invocation of .clear() on a diff is not supported; you may use .clearDiff()");
    }

    @Override
    public void add(Quad quad) {
        additions.add(quad);
        deletions.delete(quad);
    }

    @Override
    public void delete(Quad quad) {
        deletions.add(quad);
        additions.delete(quad);
    }

    @Override
    public Stream<Quad> find(Node g, Node s, Node p, Node o) {
        return Stream.concat(
                master.find(g, s, p, o).filter(q -> !deletions.contains(q)),
                additions.find(g, s, p, o).filter(q -> !master.contains(q)));
    }

    @Override
    public Stream<Node> listGraphNodes() {
        return
            Stream.concat(master.listGraphNodes(), additions.listGraphNodes()).distinct()
                .filter(g -> {
                    boolean r = true; // may become false
                    boolean hasDeletionsInG = deletions.find(g, Node.ANY, Node.ANY, Node.ANY).findAny().isPresent();
                    if (hasDeletionsInG) {
                        // For graph g test if there is any triple in master+additions that is not in deletions
                        r = Stream.concat(
                                master.find(g, Node.ANY, Node.ANY, Node.ANY),
                                additions.find(g, Node.ANY, Node.ANY, Node.ANY))
                            .filter(q -> !deletions.contains(q))
                            .findAny().isPresent();
                    }
                    return r;
                });
    }
}