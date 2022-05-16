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

package org.apache.jena.sparql.engine.iterator;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;

/**
 * Repeatedly execute the subclass operation for each Binding in the input iterator.
 */
public abstract class QueryIterRepeatApply extends QueryIterRepeatApplyBulk {

    public QueryIterRepeatApply(QueryIterator input, ExecutionContext context) {
		super(input, context);
	}

	protected abstract QueryIterator nextStage(Binding binding);

	@Override
	protected QueryIterator nextStage(QueryIterator input) {
      count++;

      if ( getInput() == null )
          return null;

      if ( !getInput().hasNext() ) {
          getInput().close();
          return null;
      }

      Binding binding = getInput().next();
      QueryIterator iter = nextStage(binding);
      return iter;

	}
}
