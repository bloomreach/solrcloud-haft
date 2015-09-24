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
package com.bloomreach.bstore.highavailability.actions;


import com.bloomreach.bstore.highavailability.zookeeper.ZkClient;
import com.bloomreach.bstore.highavailability.models.ZkBridgeData;

import java.util.Collection;

/**
 * Wrapper for Replication Configs.
 * @author nitin
 * @since 4/11/14.
 */
public class ReplicatorConfig {
  final ZkBridgeData bridgeData;
  final Collection<String> collectionNames;
  final SourceDestCollectionMapper mapper;
  private boolean skipReplicationFailures;
  private ZkClient sourceZKClient;


  /**
   * Initializes a Replicator Config.
   * @param bridgeData
   * @param collectionNames
   * @param mapper
   * @param sourceZKClient
   * @param skipReplicationFailures
   */
  public ReplicatorConfig(ZkBridgeData bridgeData, Collection<String> collectionNames, SourceDestCollectionMapper mapper, ZkClient sourceZKClient, boolean skipReplicationFailures) {
    this.bridgeData = bridgeData;
    this.collectionNames = collectionNames;
    this.mapper = mapper;
    this.sourceZKClient = sourceZKClient;
    this.skipReplicationFailures = skipReplicationFailures;
  }

  public ZkBridgeData getBridgeData() {
    return bridgeData;
  }

  public Collection<String> getCollectionNames() {
    return collectionNames;
  }

  public SourceDestCollectionMapper getMapper() {
    return mapper;
  }

  public boolean shouldSkipReplicationFailures() {
    return skipReplicationFailures;
  }

  public void setSkipReplicationFailures(boolean skipReplicationFailures) {
    this.skipReplicationFailures = skipReplicationFailures;
  }

  public ZkClient getSourceZKClient() {
    return sourceZKClient;
  }

  public void setSourceZKClient(ZkClient sourceZKClient) {
    this.sourceZKClient = sourceZKClient;
  }
}
