/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.fragment;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import java.util.List;
import java.util.Map;

/**
 * Captures parallelization parameters for a given operator/fragments. It consists of min and max width of
 * parallelization and affinity to drillbit endpoints.
 */
public class ParallelizationInfo {
  private int minWidth;
  private int maxWidth;
  private Map<DrillbitEndpoint, EndpointAffinity> affinityMap;

  private ParallelizationInfo(int minWidth, int maxWidth, Map<DrillbitEndpoint, EndpointAffinity> affinityMap) {
    this.minWidth = minWidth;
    this.maxWidth = maxWidth;
    this.affinityMap = affinityMap;
  }

  @Override
  protected ParallelizationInfo clone() {
    return new ParallelizationInfo(minWidth, maxWidth, Maps.newHashMap(affinityMap));
  }

  public static ParallelizationInfo create(int minWidth, int maxWidth) {
    return create(minWidth, maxWidth, Lists.<EndpointAffinity>newArrayList());
  }

  public static ParallelizationInfo create(int minWidth, int maxWidth, List<EndpointAffinity> endpointAffinities) {
    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = Maps.newHashMap();

    for(EndpointAffinity epAffinity : endpointAffinities) {
      affinityMap.put(epAffinity.getEndpoint(), epAffinity);
    }

    return new ParallelizationInfo(minWidth, maxWidth, affinityMap);
  }

  public void add(ParallelizationInfo parallelizationInfo) {
    this.minWidth = Math.max(minWidth, parallelizationInfo.minWidth);
    this.maxWidth = Math.min(maxWidth, parallelizationInfo.maxWidth);

    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = parallelizationInfo.getEndpointAffinityMap();
    for(Map.Entry<DrillbitEndpoint, EndpointAffinity> epAff : affinityMap.entrySet()) {
      if (this.affinityMap.containsKey(epAff.getKey())) {
        this.affinityMap.get(epAff.getKey()).addAffinity(epAff.getValue().getAffinity());
      } else {
        this.affinityMap.put(epAff.getKey(), epAff.getValue());
      }
    }
  }

  public int getMinWidth() {
    return minWidth;
  }

  public int getMaxWidth() {
    return maxWidth;
  }

  public Map<DrillbitEndpoint, EndpointAffinity> getEndpointAffinityMap() {
    return affinityMap;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("[minWidth = %d, maxWidth=%d, epAff=[", minWidth, maxWidth));
    sb.append(Joiner.on(",").join(affinityMap.values()));
    sb.append("]]");

    return sb.toString();
  }
}
