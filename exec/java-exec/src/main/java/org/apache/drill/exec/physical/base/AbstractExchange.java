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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.planner.fragment.ParallelizationInfo;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

public abstract class AbstractExchange extends AbstractSingle implements Exchange {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractExchange.class);

  protected int senderMajorFragmentId;
  protected int receiverMajorFragmentId;

  public AbstractExchange(PhysicalOperator child) {
    super(child);
  }

  /**
   * Exchanges are not executable. The Execution layer first has to set their parallelization and convert them into
   * something executable
   */
  @Override
  public boolean isExecutable() {
    return false;
  }

  /**
   * Default sender parallelization width range is [1, Integer.MAX_VALUE] and no endpoint affinity
   * @param receiverFragmentEndpoints Endpoints assigned to receiver fragment if available, otherwise an empty list.
   * @return
   */
  @Override
  public ParallelizationInfo getSenderParallelizationInfo(List<DrillbitEndpoint> receiverFragmentEndpoints) {
    return ParallelizationInfo.DEFAULT;
  }

  /**
   * Default receiver parallelization width range is [1, Integer.MAX_VALUE] and affinity to nodes where sender
   * fragments are running.
   * @param senderFragmentEndpoints Endpoints assigned to receiver fragment if available, otherwise an empty list.
   * @return
   */
  @Override
  public ParallelizationInfo getReceiverParallelizationInfo(List<DrillbitEndpoint> senderFragmentEndpoints) {
    Preconditions.checkArgument(senderFragmentEndpoints != null && senderFragmentEndpoints.size() > 0);

    return ParallelizationInfo.create(1, Integer.MAX_VALUE, getDefaultAffinityMap(senderFragmentEndpoints));
  }

  /**
   * Get a default endpoint affinity map where affinity of a Drillbit is proportional to the number of its occurrances
   * in given endpoint list.
   */
  protected List<EndpointAffinity> getDefaultAffinityMap(List<DrillbitEndpoint> fragmentEndpoints) {
    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = Maps.newHashMap();
    final double affinityPerOccurrence = 1.0d / fragmentEndpoints.size();
    for(DrillbitEndpoint sender : fragmentEndpoints) {
      if (affinityMap.containsKey(sender)) {
        affinityMap.get(sender).addAffinity(affinityPerOccurrence);
      } else {
        affinityMap.put(sender, new EndpointAffinity(sender, affinityPerOccurrence));
      }
    }

    return new ArrayList(affinityMap.values());
  }

  protected abstract void setupSenders(List<DrillbitEndpoint> senderLocations) throws PhysicalOperatorSetupException ;
  protected abstract void setupReceivers(List<DrillbitEndpoint> senderLocations) throws PhysicalOperatorSetupException ;

  @Override
  public final void setupSenders(int majorFragmentId, List<DrillbitEndpoint> senderLocations) throws PhysicalOperatorSetupException {
    this.senderMajorFragmentId = majorFragmentId;
    setupSenders(senderLocations);
  }


  @Override
  public final void setupReceivers(int majorFragmentId, List<DrillbitEndpoint> receiverLocations) throws PhysicalOperatorSetupException {
    this.receiverMajorFragmentId = majorFragmentId;
    setupReceivers(receiverLocations);
  }

  @Override
  public final <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitExchange(this, value);
  }

  @Override
  public int getOperatorType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExchangeAffinity getAffinity() {
    return ExchangeAffinity.RECEIVER_AFFINITY_TO_SENDER;
  }
}
