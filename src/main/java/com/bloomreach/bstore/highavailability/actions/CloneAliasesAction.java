/**
 * Copyright 2014-2015 BloomReach, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.bloomreach.bstore.highavailability.actions;

import com.bloomreach.bstore.highavailability.utils.SolrInteractionUtils;
import com.bloomreach.bstore.highavailability.zookeeper.ZkClient;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Clone All Aliases from Source Cluster to DestinationCluster. Refreshes the Zookeeper
 * Data after cloning and does a verification of all the aliases.
 *
 * @author nitin
 * @since 3/25/14.
 */
public class CloneAliasesAction extends SolrFaultTolerantAction {
  private ZkClient destinationZKClient;
  private final SourceDestCollectionMapper collectionMapper;

  public CloneAliasesAction(OperationConfig config) throws Exception {
    super(config);
    String collectionNameRule = this.config.getCollectionNameRule();
    String configNamePatterns = this.config.getConfigNamePatterns();
    collectionMapper = new SourceDestCollectionMapper(collectionNameRule, configNamePatterns, sourceZKClient.getZkClusterData());
    destinationZKClient = new ZkClient(config.getDestinationZkHost());
  }

  @Override
  public boolean executeAction() throws Exception {
    //Fetch all source aliases
    LinkedHashMap<String, String> sourceAliases = sourceZKClient.getZkClusterData().getAliases();
    LinkedHashMap<String, String> destinationAliases = destinationZKClient.getZkClusterData().getAliases();

    boolean dirty = false;
    logger.info("Source Cluster Alias Configuration: ");
    //Iterate over all source aliases and obtain the collections.
    for (String alias : sourceAliases.keySet()) {
      String sourceCollectionName = sourceAliases.get(alias);
      String destinationCollectionName = collectionMapper.dest(sourceCollectionName);

      if (destinationAliases.size() > 0) {
        if (destinationAliases.containsKey(alias)) {
          if (destinationAliases.get(alias).equals(destinationCollectionName)) {
            logger.info("Alias is the same in source and destination cluster for  " + alias + ". Skipping Alias Creation. ");
            continue;
          }
        }
      }

      logger.info("Existing Alias in Source Cluster....");
      dirty = true;
      String aliasLogLine = String.format("%s ->  %s", alias, sourceCollectionName);
      logger.info(aliasLogLine);

      logger.info("Alias to be Created in Destination Cluster....");
      aliasLogLine = String.format("%s ->  %s", alias, destinationCollectionName);
      logger.info(aliasLogLine);
      Set<String> destinationHosts = destinationZKClient.getZkClusterData().getSolrHosts();
      String destionationHost = (String) destinationHosts.toArray()[0];
      SolrInteractionUtils.createAlias(destionationHost, alias, destinationCollectionName);
    }

    Set<String> missingAliases = new HashSet<String>();
    //Only if anything has changed in the source zookeeper, do the verification
    if (dirty) {
      //Refresh Zookeeper view to verify if all aliases are good
      destinationZKClient.refreshZookeeperData();
      destinationAliases = destinationZKClient.getZkClusterData().getAliases();

      for (String alias : sourceAliases.keySet()) {
        if (destinationAliases.get(alias) != null) {
          logger.info("Alias " + alias + "exists on the destination cluster also...");
        } else {
          logger.error("Alias " + alias + "DOES NOT EXIST on the destination cluster. Please investigate.....");
          missingAliases.add(alias);
        }
      }
    }

    return (missingAliases.size() == 0);
  }
}
