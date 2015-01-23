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


import com.google.common.collect.Maps;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import java.util.List;
import java.util.Map;

public abstract class AbstractSender extends AbstractSingle implements Sender {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSender.class);

  protected final int oppositeMajorFragmentId;

  public AbstractSender(int oppositeMajorFragmentId, PhysicalOperator child) {
    super(child);
    this.oppositeMajorFragmentId = oppositeMajorFragmentId;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSender(this, value);
  }

  @Override
  public int getOppositeMajorFragmentId() {
    return oppositeMajorFragmentId;
  }

  /**
   * Helper method to create a mapping of fragment id and endpoint where it is running for a given endpoints
   * assignments of opposite major fragment.
   *
   * @param endpoints
   * @return
   */
  protected static Map<Integer, DrillbitEndpoint> getIndexOrderedReceiverEndpoints(List<DrillbitEndpoint> endpoints) {
    Map<Integer, DrillbitEndpoint> destinations = Maps.newHashMap();
    for(int i=0; i<endpoints.size(); i++) {
      destinations.put(i, endpoints.get(i));
    }

    return destinations;
  }
}
