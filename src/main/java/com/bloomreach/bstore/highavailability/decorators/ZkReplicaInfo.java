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

/**
 * Decorator for Replica Information in the cluster state json
 *
 * @author nitin
 * @since 1/28/14.
 */

public class ZkReplicaInfo {
  @JsonProperty("shard")
  String shardName;
  String state;
  String core;
  String collection;
  @JsonProperty("node_name")
  String nodeName;

  @JsonProperty("base_url")
  String baseUrl;
  String leader;

  public String getShardName() {
    return shardName;
  }

  public void setShardName(String shardName) {
    this.shardName = shardName;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getCore() {
    return core;
  }

  public void setCore(String core) {
    this.core = core;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public String getNodeName() {
    return nodeName;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public void setBaseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  public String getLeader() {
    return leader;
  }

  public void setLeader(String leader) {
    this.leader = leader;
  }
}
