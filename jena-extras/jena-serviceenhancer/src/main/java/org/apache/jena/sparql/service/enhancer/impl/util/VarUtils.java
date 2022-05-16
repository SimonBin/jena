/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.service.enhancer.impl.util;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ext.com.google.common.collect.BiMap;
import org.apache.jena.ext.com.google.common.collect.HashBiMap;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.Rename;

public class VarUtils {
    /**
     * Allocate a variable whose name is not in black list
     *
     * @param baseName The desired name. If in the blacklist then repeated attemps
     * with the name pattern "baseName_counter" are made until successful.
     * @param blacklistedVars Items of type Var may match
     * @return The fresh variable
     */
    public static Var freshVar(String baseName, Collection<?> blacklistedVars) {
        Var result = Var.alloc(baseName);
        int i = 0;
        while (blacklistedVars.contains(result)) {
            result = Var.alloc(baseName + "_" + ++i);
        }
        return result;
    }

    public static Map<Var, Var> reverseVarRenameMap(Collection<Var> vars) {
        Map<Var, Var> result = vars.stream()
                .collect(Collectors.toMap(
                        v -> v,
                        v -> (Var)Rename.reverseVarRename(v),
                        (v, w) -> v,
                        LinkedHashMap::new));
        return result;
    }

    public static Set<Var> reverseVarRename(Collection<Var> vars) {
        return reverseVarRename(vars, new LinkedHashSet<>());
    }

    /** Reverse-rename all variables in the given collection */
    public static <C extends Collection<? super Var>> C reverseVarRename(Collection<Var> vars, C acc) {
        for (Var v : vars) {
            Var w = (Var)Rename.reverseVarRename(v);
            acc.add(w);
        }
        return acc;
    }



    public static String getPlainName(String varName) {
        int delta = ARQConstants.allocVarScopeHiding.length();
        int pos = 0;
        while (varName.startsWith(ARQConstants.allocVarScopeHiding, pos)) {
            pos += delta;
        }
        String result = varName.substring(pos);
        return result;
    }

    public static int getScopeLevel(Var var) {
        return getScopeLevel(var.getName());
    }


    public static int getScopeLevel(String varName) {
        int result = 0;

        int delta = ARQConstants.allocVarScopeHiding.length();
        int pos = 0;
        while (varName.startsWith(ARQConstants.allocVarScopeHiding, pos)) {
            pos += delta;
            ++result;
        }

        return result;
    }

    public static Var allocScoped(String baseName, int level) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < level; ++i) {
            sb.append(ARQConstants.allocVarScopeHiding);
        }
        sb.append(baseName);
        String varName = sb.toString();
        Var result = Var.alloc(varName);
        return result;
    }



    /**
     * Creates a var mapping that pulls scoped variables up to level 0
     *
     * @param vars
     * @return
     */
    public static Map<String, Integer> getMinimumScopeLevels(Collection<Var> vars) {
        Map<String, Integer> result = new LinkedHashMap<>();
        for (Var var : vars) {
            String scopedName = var.getName();
            String plainName = getPlainName(scopedName);

            Integer priorLevel = result.get(plainName);
            int thisLevel = getScopeLevel(scopedName);

            if (priorLevel == null || thisLevel < priorLevel) {
                result.put(plainName, thisLevel);
            }
        }
        return result;
    }


    public static BiMap<Var, Var> normalizeVarScopes(Collection<Var> vars) {
        Map<String, Integer> varToMinLevel = getMinimumScopeLevels(vars);

        BiMap<Var, Var> result = HashBiMap.create();

        for (Var from : vars) {
            String fromName = from.getName();
            int fromLevel = getScopeLevel(fromName);

            String plainName = getPlainName(fromName);
            int minLevel = varToMinLevel.get(plainName);
            int normalizedLevel = fromLevel - minLevel;
            Var to = allocScoped(plainName, normalizedLevel);
            result.put(from, to);
        }

        return result;
    }

    public static BiMap<Var, Var> normalizeVarScopesGlobal(Collection<Var> vars) {
        int globalMinScopeLevel = vars.stream().mapToInt(VarUtils::getScopeLevel).min().orElse(0);

        // Reduce all scopes by the global min level
        BiMap<Var, Var> result = HashBiMap.create();
        for (Var from : vars) {
            String fromName = from.getName();
            int fromLevel = getScopeLevel(fromName);

            String plainName = getPlainName(fromName);
            int normalizedLevel = fromLevel - globalMinScopeLevel;
            Var to = allocScoped(plainName, normalizedLevel);
            result.put(from, to);
        }

        return result;

    }

}
