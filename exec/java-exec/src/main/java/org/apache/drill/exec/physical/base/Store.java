/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.base;

import java.util.List;

import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.planner.fragment.ParallelizationInfo;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * An interface which supports storing a record stream. In contrast to the logical layer, in the physical/execution
 * layers, a Store node is actually an outputting node (rather than a root node) that provides returns one or more
 * records regarding the completion of the query.
 */
public interface Store extends PhysicalOperator {

  /**
   * Inform the Store node about the actual decided DrillbitEndpoint assignments desired for storage purposes. This is a
   * precursor to the execution planner running a set of getSpecificStore() method calls for full Store node
   * materialization.
   *
   * @param endpoints
   *          The list of endpoints that this Store node are going to be executed on.
   * @throws PhysicalOperatorSetupException
   */
  public abstract void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException;

  /**
   * Provides full materialized Store operators for execution purposes.
   *
   * @param child
   *          The child operator that this operator will consume from.
   * @param minorFragmentId
   *          The particular minor fragment id associated with this particular fragment materialization.
   * @return A materialized Store Operator.
   * @throws PhysicalOperatorSetupException
   */
  public abstract Store getSpecificStore(PhysicalOperator child, int minorFragmentId)
      throws PhysicalOperatorSetupException;

  /**
   * Returns parallelization info for the Store operation. In some cases, a store operation has a limited number of
   * parallelizations that it can support and affinity to certain nodes.
   *
   * For example, a Screen return cannot be parallelized at all and can only run on node where Foreman for the query is
   * present. In this case, parallelization info includes minWidth and maxWidth value of 1 and affinity to the node
   * where Foreman is present.
   *
   * @return
   */
  @JsonIgnore
  public abstract ParallelizationInfo getParallelizationInfo();

  /**
   * Get the child of this store operator as this will be needed for parallelization materialization purposes.
   * @return
   */
  public abstract PhysicalOperator getChild();
}