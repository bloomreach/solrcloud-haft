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
package com.bloomreach.bstore.highavailability.zookeeper;

import com.bloomreach.bstore.highavailability.decorators.ZkCollectionState;
import com.bloomreach.bstore.highavailability.models.SolrCore;

import java.util.*;

/**
 * Encapsulates {@link org.apache.zookeeper} Cluster State. It contains information about the zookeeper state, aliases
 * solr Collections, solr hosts, live nodes, overseer etc.
 * </p>
 *
 * This stores multiple transformations for Solrcloud cluster data. Allows accessing cluster, collection and config data
 * across multiple dimensions.
 * </p>
 *
 * @author nitin
 * @since 3/24/14.
 */
public class ZkClusterData {
  private LinkedHashMap<String, ZkCollectionState> clusterState;
  private LinkedHashMap<String, String> aliases;
  private Collection<String> collections;
  private Set<String> solrHosts;
  private Map<String, Map<String, String>> collectionToNodeMapping;
  private Map<String, List<String>> nodeToCoreMapping;
  private Map<String, String> collectionToConfigMapping;
  private Map<String, SolrCore> coreToBestReplicaMapping;
  private Set<SolrCore> cluterCoresStatus;
  private Long clusterStateSize;
  private Collection<String> configs;
  private Map<String, String> privateIpToPublicHostNameMap;
  private Map<String, Map<String, String>> collectionToShardLeaderMapping;
  private Map<String, Map<String, Map<String, String>>> collectionToShardToCoreMapping;
  private Map<String, Map<String, String>> nodetoCoreHealthMap;

  public void setSolrHosts(Set<String> solrHosts) {
    this.solrHosts = solrHosts;
  }

  public Map<String, Map<String, String>> getNodetoCoreHealthMap() {
    return nodetoCoreHealthMap;
  }

  public void setNodetoCoreHealthMap(Map<String, Map<String, String>> nodetoCoreHealthMap) {
    this.nodetoCoreHealthMap = nodetoCoreHealthMap;
  }

  public Map<String, Map<String, Map<String, String>>> getCollectionToShardToCoreMapping() {
    return collectionToShardToCoreMapping;
  }

  public void setCollectionToShardToCoreMapping(Map<String, Map<String, Map<String, String>>> collectionToShardToCoreMapping) {
    this.collectionToShardToCoreMapping = collectionToShardToCoreMapping;
  }

  public Map<String, Map<String, String>> getCollectionToShardLeaderMapping() {
    return collectionToShardLeaderMapping;
  }

  public void setCollectionToShardLeaderMapping(Map<String, Map<String, String>> collectionToShardLeaderMapping) {
    this.collectionToShardLeaderMapping = collectionToShardLeaderMapping;
  }

  public Map<String, String> getPrivateIpToPublicHostNameMap() {
    return privateIpToPublicHostNameMap;
  }

  public void setPrivateIpToPublicHostNameMap(Map<String, String> privateIpToPublicHostNameMap) {
    this.privateIpToPublicHostNameMap = privateIpToPublicHostNameMap;
  }

  public Collection<String> getConfigs() {
    return configs;
  }

  public void setConfigs(Collection<String> configs) {
    this.configs = configs;
  }

  public LinkedHashMap<String, ZkCollectionState> getClusterState() {
    return clusterState;
  }

  public void setClusterState(LinkedHashMap<String, ZkCollectionState> clusterState) {
    this.clusterState = clusterState;
  }

  public LinkedHashMap<String, String> getAliases() {
    return aliases;
  }

  public void setAliases(LinkedHashMap<String, String> aliases) {
    this.aliases = aliases;
  }

  public Collection<String> getCollections() {
    return collections;
  }

  public void setCollections(Collection<String> collections) {
    this.collections = collections;
  }

  public Set<String> getSolrHosts() {
    return solrHosts;
  }

  public void updateSolrNodes(Set<String> solrHosts) {
    if (this.solrHosts != null) {
      this.solrHosts.clear();
    }
    this.solrHosts = solrHosts;
  }

  public Map<String, Map<String, String>> getCollectionToNodeMapping() {
    return collectionToNodeMapping;
  }

  public void setCollectionToNodeMapping(Map<String, Map<String, String>> collectionToNodeMapping) {
    this.collectionToNodeMapping = collectionToNodeMapping;
  }

  public Map<String, List<String>> getNodeToCoreMapping() {
    return nodeToCoreMapping;
  }

  public void setNodeToCoreMapping(Map<String, List<String>> nodeToCoreMapping) {
    this.nodeToCoreMapping = nodeToCoreMapping;
  }

  public Map<String, String> getCollectionToConfigMapping() {
    return collectionToConfigMapping;
  }

  public void setCollectionToConfigMapping(Map<String, String> collectionToConfigMapping) {
    this.collectionToConfigMapping = collectionToConfigMapping;
  }

  public Map<String, SolrCore> getCoreToBestReplicaMapping() {
    return coreToBestReplicaMapping;
  }

  public void setCoreToBestReplicaMapping(Map<String, SolrCore> coreToBestReplicaMapping) {
    this.coreToBestReplicaMapping = coreToBestReplicaMapping;
  }

  public Set<SolrCore> getCluterCoresStatus() {
    return cluterCoresStatus;
  }

  public void setCluterCoresStatus(Set<SolrCore> cluterCoresStatus) {
    this.cluterCoresStatus = cluterCoresStatus;
  }

  public Long getClusterStateSize() {
    return clusterStateSize;
  }

  public void setClusterStateSize(Long clusterStateSize) {
    this.clusterStateSize = clusterStateSize;
  }

}
