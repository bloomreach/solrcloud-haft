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

import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.LinkedHashMap;

/**
 * Uber Decorator for a collection state in cluster state json.
 *
 * @author nitin
 * @since 1/28/14.
 */
public class ZkCollectionState {
  LinkedHashMap<String, ZkShardInfo> shards;
  @JsonIgnore
  Router router;
  String maxShardsPerNode;
  String replicationFactor;
  @JsonIgnore
  String autoCreated;

  public String getMaxShardsPerNode() {
    return maxShardsPerNode;
  }

  public void setMaxShardsPerNode(String maxShardsPerNode) {
    this.maxShardsPerNode = maxShardsPerNode;
  }

  public String getReplicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(String replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public LinkedHashMap<String, ZkShardInfo> getShards() {
    return shards;
  }

  public void setShards(LinkedHashMap<String, ZkShardInfo> shards) {
    this.shards = shards;
  }

  public Router getRouter() {
    return router;
  }

  public void setRouter(Router router) {
    this.router = router;
  }

  public String getAutoCreated() {
    return autoCreated;
  }

  public void setAutoCreated(String autoCreated) {
    this.autoCreated = autoCreated;
  }
}
