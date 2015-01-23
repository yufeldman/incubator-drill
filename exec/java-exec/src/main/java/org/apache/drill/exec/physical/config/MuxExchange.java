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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
 * Multiplexing Exchange (MuxExchange) is used when results from multiple minor fragments belonging to the same
 * major fragment running on a node need to be collected at one fragment on the same node before distributing the
 * results further. This helps when the sender that is distributing the results has overhead that is proportional to
 * the number of sender instances. An example of such sender is PartitionSender. Each instance of PartitionSender
 * allocates "r" buffers where "r" is the number of receivers.
 *
 * Ex. Drillbit A is assigned 10 minor fragments belonging to the same major fragment. Each of these fragments
 * has a PartitionSender instance which is sending data to 300 receivers. Each PartitionSender needs 300 buffers,
 * so total of 10*300 buffers are needed. With MuxExchange, all 10 fragments send the data directly (without
 * partitioning) to MuxExchange which uses the PartitionSender to partition the incoming data and distribute
 * to receivers. MuxExchange has only one instance per Drillbit per major fragment which means only one instance of
 * PartitionSender per Drillbit per major fragment. With MuxExchange total number of buffers used by PartitionSender
 * for the 10 fragments is 300 instead of earlier number 10*300.
 */
@JsonTypeName("mux-exchange")
public class MuxExchange extends AbstractExchange {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MuxExchange.class);

  private List<DrillbitEndpoint> senderLocations;
  private List<DrillbitEndpoint> receiverLocations;

  @JsonIgnore
  private boolean isSenderReceiverMappingCreated = false;

  @JsonIgnore
  private Map<Integer, MinorFragmentEndpoint> senderToReceiverMapping;

  @JsonIgnore
  private ArrayListMultimap<Integer, MinorFragmentEndpoint> receiverToSenderMapping;

  public MuxExchange(@JsonProperty("child") PhysicalOperator child) {
    super(child);
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
  public ParallelizationInfo getReceiverParallelizationInfo(List<DrillbitEndpoint> senderFragmentEndpoints) {
    Preconditions.checkArgument(senderFragmentEndpoints != null && senderFragmentEndpoints.size() > 0);

    // We want to run one mux receiver per Drillbit endpoint.
    // Identify the number of unique Drillbit endpoints in sender fragment endpoints.
    List<DrillbitEndpoint> drillbitEndpoints = ImmutableSet.copyOf(senderFragmentEndpoints).asList();

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
    MinorFragmentEndpoint receiver = senderToReceiverMapping.get(minorFragmentId);
    Preconditions.checkState(receiver != null,
        String.format("Failed to find receiver for sender [%d]", minorFragmentId));

    return new SingleSender(receiverMajorFragmentId, receiver.getId(), child, receiver.getEndpoint());
  }

  @Override
  public Receiver getReceiver(int minorFragmentId) {
    if (!isSenderReceiverMappingCreated) {
      createSenderReceiverMapping();
    }
    List<MinorFragmentEndpoint> senders = receiverToSenderMapping.get(minorFragmentId);
    Preconditions.checkState(senders != null && senders.size() > 0,
        String.format("Failed to find senders for receiver [%d]", minorFragmentId));

    return new UnorderedReceiver(senderMajorFragmentId, senders);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new MuxExchange(child);
  }

  /**
   * In MuxExchange, receivers have affinity to where the sending fragments are running.
   */
  @Override
  public ExchangeAffinity getAffinity() {
    return ExchangeAffinity.RECEIVER_AFFINITY_TO_SENDER;
  }

  private void createSenderReceiverMapping() {
    senderToReceiverMapping = Maps.newHashMap();
    receiverToSenderMapping = ArrayListMultimap.create();

    // Create a list of sender fragment ids for each Drillbit endpoint
    ArrayListMultimap<DrillbitEndpoint, Integer> endpointSenderList = ArrayListMultimap.create();
    for(int senderFragmentId = 0; senderFragmentId < senderLocations.size(); senderFragmentId++) {
      DrillbitEndpoint senderDrillbitLocation = senderLocations.get(senderFragmentId);
      endpointSenderList.put(senderDrillbitLocation, senderFragmentId);
    }

    for(int receiverFragmentId = 0; receiverFragmentId < receiverLocations.size(); receiverFragmentId++) {
      List<Integer> senderFragmentIds = endpointSenderList.get(receiverLocations.get(receiverFragmentId));

      for(Integer senderFragmentId : senderFragmentIds) {
        senderToReceiverMapping.put(senderFragmentId,
            new MinorFragmentEndpoint(receiverFragmentId, receiverLocations.get(receiverFragmentId)));

        receiverToSenderMapping.put(receiverFragmentId,
            new MinorFragmentEndpoint(senderFragmentId, senderLocations.get(senderFragmentId)));
      }
    }

    isSenderReceiverMappingCreated = true;
  }
}
