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

package com.bloomreach.bstore.highavailability.clients;

import com.bloomreach.bstore.highavailability.actions.FTActions;
import com.bloomreach.bstore.highavailability.actions.OperationConfig;
import com.bloomreach.bstore.highavailability.actions.SolrActionFactory;
import com.bloomreach.bstore.highavailability.actions.SolrFaultTolerantAction;
import com.bloomreach.bstore.highavailability.exceptions.ClusterCloneException;
import com.bloomreach.bstore.highavailability.exceptions.CollectionNotFoundException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Given a source ZK and destination Zk, recovers the source cluster information onto
 * the destination cluster.
 *
 * @author nitin
 * @since 06/02/2014
 */
public class SolrCloudCrossClusterBackupWithNewCollectionNames {
  protected static final Logger logger = Logger.getLogger(SolrCloudCrossClusterBackupWithNewCollectionNames.class);

  public static void main(String[] args) throws Exception {

    String sourceZk = args[0];
    String destinationZookeeper = args[1];

    //Define Zk Cloning Config Action.
    OperationConfig config = constructZkCloneConfig(sourceZk, destinationZookeeper);

    //Execute the Zk Clone Action.
    SolrFaultTolerantAction zkConfigsAction = SolrActionFactory.fetchActionToPerform(config);
    zkConfigsAction.executeAction();

    //Define SolrCloud Collection Cloning Action.
    OperationConfig cloneCollectionConfig = constructClusterCloneConfig(sourceZk, destinationZookeeper);
    logger.info("Using " + sourceZk + " as the source zookeeper");
    logger.info("Using " + destinationZookeeper + " as the destination zookeeper");
    try {
      logger.info("Streaming data from Source cluster to Destination Solr Cluster ...");
      //Execute solrCloud Collection Cloning
      SolrFaultTolerantAction cloneAction = SolrActionFactory.fetchActionToPerform(cloneCollectionConfig);
      cloneAction.executeAction();
      logger.info("Streaming Succesful from " + sourceZk + " cluster to " + destinationZookeeper + " Solr Cluster");
    } catch (Exception e) {
      logger.info("Failed to stream collection:" + ExceptionUtils.getFullStackTrace(e));
      throw new ClusterCloneException(ExceptionUtils.getFullStackTrace(e));
    }
  }


  /**
   * Everything in HAFT is controlled by the {@link com.bloomreach.bstore.highavailability.actions.OperationConfig}. It dictates
   * the input for every action. The main input knobs include
   * 1. setSourceZk : Sets the source Zookeeper instance to read the configs from.
   * 2. setAction   : Set the desired action to be performed. Instance of {@link com.bloomreach.bstore.highavailability.actions.FTActions}
   * 3. setDestinationZK : Destination Zookeeper to replicate the configs into.
   * 4. setCollections : List of collections to be cloned. Specify all for everything.
   * 5. setReplicationFactor : Replication Factor for the destination collections.
   * @param sourceZk
   * @param destinationZookeeper
   * @return OperationConfig Full operation config for Collection Cloning
   */
  private static OperationConfig constructClusterCloneConfig(String sourceZk, String destinationZookeeper) throws IOException, InterruptedException, KeeperException, CollectionNotFoundException {
    List<String> solrCollections = new ArrayList<String>();
    solrCollections.add("all");

    //Define the operation Config for HAFT. Determines Collections, Replication Factor etc
    OperationConfig cloneCollectionConfig = new OperationConfig();
    cloneCollectionConfig.setZkHost(sourceZk);
    cloneCollectionConfig.setDestinationZkHost(destinationZookeeper);
    cloneCollectionConfig.setCollections(solrCollections);
    cloneCollectionConfig.setReplicationFactor("4");
    //TODO: Change this based on your setup.
    cloneCollectionConfig.setCollectionNameRule("_collection:_collectionnew,default:_collectionnew");
    cloneCollectionConfig.setConfigNamePatterns("collection:test_config,_collectionnew:test_config,default:test_config");
    cloneCollectionConfig.setAction(FTActions.CLONE.getAction());
    return cloneCollectionConfig;
  }

  /**
   * Everything in HAFT is controlled by the {@link com.bloomreach.bstore.highavailability.actions.OperationConfig}. It dictates
   * the input for every action. The main input knobs include
   * 1. setSourceZk : Sets the source Zookeeper instance to read the configs from.
   * 2. setAction   : Set the desired action to be performed. Instance of {@link com.bloomreach.bstore.highavailability.actions.FTActions}
   * 3. setDestinationZK : Destination Zookeeper to replicate the configs into.
   * @param sourceZk
   * @param destinationZookeeper
   * @return OperationConfig Full operation config for Zk Cloning
   */
  private static OperationConfig constructZkCloneConfig(String sourceZk, String destinationZookeeper) {
    OperationConfig config = new OperationConfig();
    config.setZkHost(sourceZk);
    //Indicating
    config.setConfigRoot("/configs");
    config.setDestinationZkHost(destinationZookeeper);
    config.setAction(FTActions.CLONE_ZK.getAction());
    return config;
  }
}
