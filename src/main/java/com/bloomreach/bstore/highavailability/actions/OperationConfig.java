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

import java.util.List;

/**
 * Generic Operation Config to wrap parameters for all HAFT Actions.
 *
 * @author nitin
 * @since 2/19/14.
 */
public class OperationConfig {
  public OperationConfig() {

  }
  private String sourceSolrHost;
  private String destinationSolrHost;
  private String action;
  private String zkHost;
  private String destinationZkHost;
  private String desiredShards;
  private List<String> collections;
  private boolean dryRun;
  private String alias;
  private String collectionNameRule;
  private String configNamePatterns;
  private String replicationFactor;
  private boolean skipReplicationFailures;
  private String configName;
  private String streamFilter;
  private String exclusionPattern;
  private String collectionVersion;
  private List<String> solrHosts;
  private String configRoot;

  public String getConfigRoot() {
    return configRoot;
  }

  public void setConfigRoot(String configRoot) {
    this.configRoot = configRoot;
  }


  public String getExclusionPattern() {
    return exclusionPattern;
  }

  public void setExclusionPattern(String exclusionPattern) {
    this.exclusionPattern = exclusionPattern;
  }


  public String getConfigName() {
    return configName;
  }

  public void setConfigName(String configName) {
    this.configName = configName;
  }

  public boolean isSkipReplicationFailures() {
    return skipReplicationFailures;
  }

  public void setSkipReplicationFailures(boolean skipReplicationFailures) {
    this.skipReplicationFailures = skipReplicationFailures;
  }

  public String getDesiredShards() {
    return desiredShards;
  }

  public void setDesiredShards(String desiredShards) {
    this.desiredShards = desiredShards;
  }

  public String getSourceSolrHost() {
    return sourceSolrHost;
  }

  public void setSourceSolrHost(String sourceSolrHost) {
    this.sourceSolrHost = sourceSolrHost;
  }

  public String getDestinationSolrHost() {
    return destinationSolrHost;
  }

  public void setDestinationSolrHost(String destinationSolrHost) {
    this.destinationSolrHost = destinationSolrHost;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public String getZkHost() {
    return zkHost;
  }

  public void setZkHost(String zkHost) {
    this.zkHost = zkHost;
  }

  public List<String> getCollections() {
    return collections;
  }

  public void setCollections(List<String> collections) {
    this.collections = collections;
  }

  public boolean isDryRun() {
    return dryRun;
  }

  public void setDryRun(boolean dryRun) {
    this.dryRun = dryRun;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public String getDestinationZkHost() {
    return destinationZkHost;
  }

  public void setDestinationZkHost(String destinationZkHost) {
    this.destinationZkHost = destinationZkHost;
  }

  public String getCollectionNameRule() {
    return collectionNameRule;
  }

  public void setCollectionNameRule(String collectionNameRule) {
    this.collectionNameRule = collectionNameRule;
  }

  public String getConfigNamePatterns() {
    return configNamePatterns;
  }

  public void setConfigNamePatterns(String configNamePatterns) {
    this.configNamePatterns = configNamePatterns;
  }

  public String getReplicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(String replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public String getStreamFilter() {
    return streamFilter;
  }

  public void setStreamFilter(String streamFilter) {
    this.streamFilter = streamFilter;
  }


  public String getCollectionVersion() {
    return collectionVersion;
  }

  public void setCollectionVersion(String collectionVersion) {
    this.collectionVersion = collectionVersion;
  }


  public void setSolrHosts(List<String> solrHosts) {
    this.solrHosts = solrHosts;
  }

  public List<String> getSolrHosts() {
    return solrHosts;
  }
}
