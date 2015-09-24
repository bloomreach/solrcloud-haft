/**
 * Copyright 2014-2015 BloomReach, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bloomreach.bstore.highavailability.decorators;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.LinkedHashMap;

/**
 * Decorator for Shard Information in the cluster state json
 *
 * @author nitin
 * @since 1/28/14.
 */
public class ZkShardInfo {
  String range;
  String state;
  String parent;

  @JsonProperty("replicas")
  LinkedHashMap<String, ZkReplicaInfo> replicas;

  public String getRange() {
    return range;
  }

  public void setRange(String range) {
    this.range = range;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public LinkedHashMap<String, ZkReplicaInfo> getReplicas() {
    return replicas;
  }

  public void setReplicas(LinkedHashMap<String, ZkReplicaInfo> replicas) {
    this.replicas = replicas;
  }

  public String getParent() {
    return parent;
  }

  public void setParent(String parent) {
    this.parent = parent;
  }
}
