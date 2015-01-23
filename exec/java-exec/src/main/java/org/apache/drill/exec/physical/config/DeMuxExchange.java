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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.EndpointAffinity;
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
 *
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
  private Map<Integer, MinorFragmentIdEndpointPair> receiverToSenderMapping;

  @JsonIgnore
  private Map<Integer, List<MinorFragmentIdEndpointPair>> senderToReceiversMapping;

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
  public Sender getSender(int minorFragmentId, PhysicalOperator child) {
    if (!isSenderReceiverMappingCreated) {
      createSenderReceiverMapping();
    }

    List<MinorFragmentIdEndpointPair> receivers = senderToReceiversMapping.get(minorFragmentId);
    if (receivers == null) {
      throw new RuntimeException(String.format("Failed to find receivers for sender [%d]", minorFragmentId));
    }

    Map<Integer, DrillbitEndpoint> receiverMap = Maps.newHashMap();
    for(MinorFragmentIdEndpointPair receiver : receivers) {
      receiverMap.put(receiver.getMinorFragmentId(), receiver.getEndpoint());
    }

    return new HashPartitionSender(receiverMajorFragmentId, child, expr, receiverMap);
  }

  @Override
  public Receiver getReceiver(int minorFragmentId) {
    if (!isSenderReceiverMappingCreated) {
      createSenderReceiverMapping();
    }

    MinorFragmentIdEndpointPair sender = receiverToSenderMapping.get(minorFragmentId);
    if (sender == null) {
      throw new RuntimeException(String.format("Failed to find sender for receiver [%d]", minorFragmentId));
    }

    Map<Integer, DrillbitEndpoint> senderMap = Maps.newHashMap();
    senderMap.put(sender.getMinorFragmentId(), sender.getEndpoint());

    return new UnorderedReceiver(this.senderMajorFragmentId, senderMap);
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
    senderToReceiversMapping = Maps.newHashMap();
    receiverToSenderMapping = Maps.newHashMap();

    Map<DrillbitEndpoint, List<Integer>> endpointReceiverList = Maps.newHashMap();
    for(int i = 0; i < receiverLocations.size(); i++) {
      DrillbitEndpoint senderDrillbitLocation = receiverLocations.get(i);
      if (endpointReceiverList.containsKey(senderDrillbitLocation)) {
        endpointReceiverList.get(senderDrillbitLocation).add(i);
      } else {
        endpointReceiverList.put(senderDrillbitLocation, Lists.newArrayList(i));
      }
    }

    for(int senderFragmentId=0; senderFragmentId<senderLocations.size(); senderFragmentId++) {
      List<Integer> receiverMinorFragmentIds = endpointReceiverList.get(senderLocations.get(senderFragmentId));

      List<MinorFragmentIdEndpointPair> receivers = Lists.newArrayList();
      for(Integer receiverFragmentId : receiverMinorFragmentIds) {
        receivers.add(new MinorFragmentIdEndpointPair(receiverFragmentId, receiverLocations.get(receiverFragmentId)));
        receiverToSenderMapping.put(receiverFragmentId,
            new MinorFragmentIdEndpointPair(senderFragmentId, senderLocations.get(senderFragmentId)));
      }

      senderToReceiversMapping.put(senderFragmentId, receivers);
    }

    isSenderReceiverMappingCreated = true;
  }

  private class MinorFragmentIdEndpointPair {
    private final int minorFragmentId;
    private final DrillbitEndpoint endpoint;

    public MinorFragmentIdEndpointPair(int minorFragmentId, DrillbitEndpoint endpoint) {
      this.minorFragmentId = minorFragmentId;
      this.endpoint = endpoint;
    }

    public int getMinorFragmentId() {
      return minorFragmentId;
    }

    public DrillbitEndpoint getEndpoint() {
      return endpoint;
    }
  }
}
