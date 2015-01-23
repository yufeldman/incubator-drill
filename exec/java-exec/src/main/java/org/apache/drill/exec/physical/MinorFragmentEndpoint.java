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
package org.apache.drill.exec.physical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

@JsonTypeName("fragment-endpoint")
public class MinorFragmentEndpoint {
  private final int id;
  private final DrillbitEndpoint endpoint;

  @JsonCreator
  public MinorFragmentEndpoint(@JsonProperty("minorFragmentId") int id, @JsonProperty("endpoint") DrillbitEndpoint endpoint) {
    this.id = id;
    this.endpoint = endpoint;
  }

  @JsonProperty("minorFragmentId")
  public int getId() {
    return id;
  }

  @JsonProperty("endpoint")
  public DrillbitEndpoint getEndpoint() {
    return endpoint;
  }

  @JsonIgnore
  @Override
  public String toString() {
    return "FragmentEndPoint: id = " + id + ", ep = " + endpoint;
  }
}
