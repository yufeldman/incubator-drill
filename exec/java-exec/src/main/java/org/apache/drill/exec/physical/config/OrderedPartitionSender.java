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

import com.google.common.collect.Maps;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.physical.base.AbstractSender;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;

@JsonTypeName("OrderedPartitionSender")
public class OrderedPartitionSender extends AbstractSender {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderedPartitionSender.class);

  private final List<Ordering> orderings;
  private final FieldReference ref;
  private final Map<Integer, DrillbitEndpoint> endpoints;
  private final int sendingWidth;

  private int recordsToSample;
  private int samplingFactor;
  private float completionFactor;

  /**
   * Creates OrderedPartitionSender which sends data to all minor fragments in receiving major fragment.
   *
   * @param orderings
   * @param ref
   * @param child
   * @param endpoints This list should be index-ordered the same as the expected minorFragmentIds for each receiver.
   * @param oppositeMajorFragmentId
   * @param sendingWidth
   * @param recordsToSample
   * @param samplingFactor
   * @param completionFactor
   */
  public OrderedPartitionSender(List<Ordering> orderings,
      FieldReference ref,
      PhysicalOperator child,
      List<DrillbitEndpoint> endpoints,
      int oppositeMajorFragmentId,
      int sendingWidth,
      int recordsToSample,
      int samplingFactor,
      float completionFactor) {
    this(orderings, ref, child, getIndexOrderedReceiverEndpoints(endpoints), oppositeMajorFragmentId, sendingWidth,
        recordsToSample, samplingFactor, completionFactor);
  }

  @JsonCreator
  public OrderedPartitionSender(@JsonProperty("orderings") List<Ordering> orderings,
                                @JsonProperty("ref") FieldReference ref,
                                @JsonProperty("child") PhysicalOperator child,
                                @JsonProperty("destinations") Map<Integer, DrillbitEndpoint> endpoints,
                                @JsonProperty("receiver-major-fragment") int oppositeMajorFragmentId,
                                @JsonProperty("sending-fragment-width") int sendingWidth,
                                @JsonProperty("recordsToSample") int recordsToSample,
                                @JsonProperty("samplingFactor") int samplingFactor,
                                @JsonProperty("completionFactor") float completionFactor) {
    super(oppositeMajorFragmentId, child);
    if (orderings == null) {
      this.orderings = Lists.newArrayList();
    } else {
      this.orderings = orderings;
    }
    this.ref = ref;
    this.endpoints = endpoints;
    this.sendingWidth = sendingWidth;
    this.recordsToSample = recordsToSample;
    this.samplingFactor = samplingFactor;
    this.completionFactor = completionFactor;
  }

  public int getSendingWidth() {
    return sendingWidth;
  }

  public Map<Integer, DrillbitEndpoint> getDestinations() {
    return endpoints;
  }

  public List<Ordering> getOrderings() {
    return orderings;
  }

  public FieldReference getRef() {
    return ref;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitOrderedPartitionSender(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new OrderedPartitionSender(orderings, ref, child, endpoints, oppositeMajorFragmentId, sendingWidth, recordsToSample, samplingFactor,
            completionFactor);
  }

  public int getRecordsToSample() {
    return recordsToSample;
  }

  public int getSamplingFactor() {
    return samplingFactor;
  }

  public float getCompletionFactor() {
    return completionFactor;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.ORDERED_PARTITION_SENDER_VALUE;
  }
}
