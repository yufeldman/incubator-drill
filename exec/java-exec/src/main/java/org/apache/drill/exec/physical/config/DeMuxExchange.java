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
package org.apache.drill.exec.physical.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractExchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Receiver;
import org.apache.drill.exec.physical.base.Sender;
import org.apache.drill.exec.planner.fragment.ParallelizationInfo;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * DeMuxExchange is opposite of MuxExchange. It is used when the sender has overhead that is proportional to the
 * number of receivers. DeMuxExchange is run one instance per Drillbit endpoint which collects and distributes data
 * belonging to local receiving fragments running on the same Drillbit.
 *
 * Example:
 * On a 3 node cluster, if the sender has 10 receivers on each node each sender requires 30 buffers. By inserting
 * DeMuxExchange, we create one receiver per node which means total of 3 receivers for each sender. If the number of
 * senders is 10, we use 10*3 buffers instead of 10*30. DeMuxExchange has a overhead of buffer space that is equal to
 * number of local receivers. In this case each DeMuxExchange needs 10 buffers, so total of 3*10 buffers.
 */
@JsonTypeName("demux-exchange")
public class DeMuxExchange extends AbstractExchange {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DeMuxExchange.class);

  private final LogicalExpression expr;

  private List<DrillbitEndpoint> senderLocations;
  private List<DrillbitEndpoint> receiverLocations;

  @JsonIgnore
  private boolean isSenderReceiverMappingCreated = false;

  @JsonIgnore
  private Map<Integer, MinorFragmentEndpoint> receiverToSenderMapping;

  @JsonIgnore
  private ArrayListMultimap<Integer, MinorFragmentEndpoint> senderToReceiversMapping;

  public DeMuxExchange(@JsonProperty("child") PhysicalOperator child, @JsonProperty("expr") LogicalExpression expr) {
    super(child);
    this.expr = expr;
  }

  @JsonProperty("expr")
  public LogicalExpression getExpression(){
    return expr;
  }

  @Override
  public void setupSenders(List<DrillbitEndpoint> senderLocations) {
    this.senderLocations = senderLocations;
  }

  @Override
  protected void setupReceivers(List<DrillbitEndpoint> receiverLocations) throws PhysicalOperatorSetupException {
    this.receiverLocations = receiverLocations;
  }

  @Override
  public ParallelizationInfo getSenderParallelizationInfo(List<DrillbitEndpoint> receiverFragmentEndpoints) {
    // Make sure the receiver fragment endpoint list is not empty
    Preconditions.checkArgument(receiverFragmentEndpoints != null && receiverFragmentEndpoints.size() > 0);

    // We want to run one demux sender per Drillbit endpoint.
    // Identify the number of unique Drillbit endpoints in receiver fragment endpoints.
    List<DrillbitEndpoint> drillbitEndpoints = ImmutableSet.copyOf(receiverFragmentEndpoints).asList();

    List<EndpointAffinity> affinities = Lists.newArrayList();
    for(DrillbitEndpoint ep : drillbitEndpoints) {
      affinities.add(new EndpointAffinity(ep, Double.POSITIVE_INFINITY));
    }

    return ParallelizationInfo.create(affinities.size(), affinities.size(), affinities);
  }

  @Override
  public ParallelizationInfo getReceiverParallelizationInfo(List<DrillbitEndpoint> senderFragmentEndpoints) {
    return ParallelizationInfo.DEFAULT;
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child) {
    if (!isSenderReceiverMappingCreated) {
      createSenderReceiverMapping();
    }

    List<MinorFragmentEndpoint> receivers = senderToReceiversMapping.get(minorFragmentId);
    Preconditions.checkState(receivers != null, String.format("Failed to find receivers for sender [%d]", minorFragmentId));

    return new HashPartitionSender(receiverMajorFragmentId, child, expr, receivers);
  }

  @Override
  public Receiver getReceiver(int minorFragmentId) {
    if (!isSenderReceiverMappingCreated) {
      createSenderReceiverMapping();
    }

    MinorFragmentEndpoint sender = receiverToSenderMapping.get(minorFragmentId);
    Preconditions.checkState(sender != null, String.format("Failed to find sender for receiver [%d]", minorFragmentId));

    return new UnorderedReceiver(this.senderMajorFragmentId, Collections.singletonList(sender));
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new DeMuxExchange(child, expr);
  }

  /**
   * In DeMuxExchange, senders have affinity to where the receiving fragments are running.
   */
  @Override
  public ExchangeAffinity getAffinity() {
    return ExchangeAffinity.SENDER_AFFINITY_TO_RECEIVER;
  }

  private void createSenderReceiverMapping() {
    senderToReceiversMapping = ArrayListMultimap.create();
    receiverToSenderMapping = Maps.newHashMap();

    // Create a list of receiver fragment ids for each Drillbit endpoint
    ArrayListMultimap<DrillbitEndpoint, Integer> endpointReceiverList = ArrayListMultimap.create();

    for(int receiverFragmentId = 0; receiverFragmentId < receiverLocations.size(); receiverFragmentId++) {
      DrillbitEndpoint receiverDrillbitLocation = receiverLocations.get(receiverFragmentId);
      endpointReceiverList.put(receiverDrillbitLocation, receiverFragmentId);
    }

    for(int senderFragmentId = 0; senderFragmentId < senderLocations.size(); senderFragmentId++) {
      List<Integer> receiverMinorFragmentIds = endpointReceiverList.get(senderLocations.get(senderFragmentId));

      for(Integer receiverFragmentId : receiverMinorFragmentIds) {
        receiverToSenderMapping.put(receiverFragmentId,
            new MinorFragmentEndpoint(senderFragmentId, senderLocations.get(senderFragmentId)));
        senderToReceiversMapping.put(senderFragmentId,
            new MinorFragmentEndpoint(receiverFragmentId, receiverLocations.get(receiverFragmentId)));
      }
    }

    isSenderReceiverMappingCreated = true;
  }
}
