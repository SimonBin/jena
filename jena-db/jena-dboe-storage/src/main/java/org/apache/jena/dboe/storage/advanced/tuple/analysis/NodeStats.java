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

package org.apache.jena.dboe.storage.advanced.tuple.analysis;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 *
 * @author Claus Stadler 11/09/2020
 *
 * @param <D>
 * @param <C>
 */
public class NodeStats<D, C> {
    protected StoreAccessor<D, C> accessor;
    protected com.github.andrewoma.dexx.collection.List<Integer> matchedConstraintIdxs;
    protected com.github.andrewoma.dexx.collection.List<Integer> matchedProjectIdxs;

    protected Set<Integer> matchedConstraintIdxSet;
    protected Set<Integer> matchedProjectIdxSet;

//    public NodeStats(StoreAccessor<D, C> accessor, int[] matches) {
//        super();
//        this.accessor = accessor;
//        this.matches = matches;
//    }

    public NodeStats(
            StoreAccessor<D, C> accessor,
            com.github.andrewoma.dexx.collection.List<Integer> matchedConstraintIdxs,
            com.github.andrewoma.dexx.collection.List<Integer> matchedProjectIdxs) {
        super();
        this.accessor = accessor;
        this.matchedConstraintIdxs = matchedConstraintIdxs;
        this.matchedProjectIdxs = matchedProjectIdxs;

        this.matchedConstraintIdxSet = new LinkedHashSet<>(matchedConstraintIdxs.asList());
        this.matchedProjectIdxSet = new LinkedHashSet<>(matchedProjectIdxs.asList());
    }



    /** Component indices */
     // protected Set<Integer> matches;

    public com.github.andrewoma.dexx.collection.List<Integer> getMatchedConstraintIdxs() {
        return matchedConstraintIdxs;
    }

    public com.github.andrewoma.dexx.collection.List<Integer> getMatchedProjectIdxs() {
        return matchedProjectIdxs;
    }

    public Set<Integer> getMatchedConstraintIdxSet() {
        return matchedConstraintIdxSet;
    }

    public Set<Integer> getMatchedProjectIdxSet() {
        return matchedProjectIdxSet;
    }

    public StoreAccessor<D, C> getAccessor() {
        return accessor;
    }



    @Override
    public String toString() {
        return "NodeStats [accessor=" + accessor + ", matchedConstraintIdxs=" + matchedConstraintIdxs
                + ", matchedProjectIdxs=" + matchedProjectIdxs + ", matchedConstraintIdxSet=" + matchedConstraintIdxSet
                + ", matchedProjectIdxSet=" + matchedProjectIdxSet + "]";
    }

}