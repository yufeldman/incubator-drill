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
package org.apache.drill.exec.physical;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.protobuf.TextFormat;


public class EndpointAffinity implements Comparable<EndpointAffinity>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EndpointAffinity.class);

  private DrillbitEndpoint endpoint;
  private double affinity = 0.0d;

  public EndpointAffinity(DrillbitEndpoint endpoint) {
    super();
    this.endpoint = endpoint;
  }

  public EndpointAffinity(DrillbitEndpoint endpoint, double affinity) {
    super();
    this.endpoint = endpoint;
    this.affinity = affinity;
  }

  public DrillbitEndpoint getEndpoint() {
    return endpoint;
  }
  public void setEndpoint(DrillbitEndpoint endpoint) {
    this.endpoint = endpoint;
  }
  public double getAffinity() {
    return affinity;
  }

  @Override
  public int compareTo(EndpointAffinity o) {
    return Double.compare(affinity, o.affinity);
  }

  public void addAffinity(double f){
    if (Double.POSITIVE_INFINITY == f) {
      affinity = f;
    } else if (Double.POSITIVE_INFINITY != affinity) {
      affinity += f;
    }
  }

  public boolean nodeShouldPresent() {
    return Double.POSITIVE_INFINITY == affinity;
  }

  @Override
  public String toString() {
    return "EndpointAffinity [endpoint=" + TextFormat.shortDebugString(endpoint) + ", affinity=" + affinity + "]";
  }

}