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

import com.google.common.collect.ImmutableMap;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.base.AbstractSender;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Sender that pushes all data to a single destination node.
 */
@JsonTypeName("single-sender")
public class SingleSender extends AbstractSender {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleSender.class);

  @JsonCreator
  public SingleSender(@JsonProperty("receiver-major-fragment") int oppositeMajorFragmentId,
                      @JsonProperty("receiver-minor-fragment") int oppositeMinorFragmentId,
                      @JsonProperty("child") PhysicalOperator child,
                      @JsonProperty("destination") DrillbitEndpoint destination) {
    super(oppositeMajorFragmentId, child,
        Collections.singletonList(new MinorFragmentEndpoint(oppositeMinorFragmentId, destination)));
  }

  public SingleSender(int oppositeMajorFragmentId, PhysicalOperator child, DrillbitEndpoint destination) {
    this(oppositeMajorFragmentId, 0 /* default opposite minor fragment id*/, child, destination);
  }

  @Override
  @JsonIgnore // Destination endpoint is exported via getDestination() and getOppositeMinorFragmentId()
  public List<MinorFragmentEndpoint> getDestinations() {
    return destinations;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new SingleSender(oppositeMajorFragmentId, getOppositeMinorFragmentId(), child, getDestination());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSingleSender(this, value);
  }

  @JsonProperty("destination")
  public DrillbitEndpoint getDestination() {
    return getDestinations().iterator().next().getEndpoint();
  }

  @JsonProperty("receiver-minor-fragment")
  public int getOppositeMinorFragmentId() {
    return getDestinations().iterator().next().getId();
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.SINGLE_SENDER_VALUE;
  }

}
