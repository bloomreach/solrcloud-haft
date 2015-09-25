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

import com.bloomreach.bstore.highavailability.exceptions.*;
import com.bloomreach.bstore.highavailability.utils.SolrInteractionUtils;
import com.bloomreach.bstore.highavailability.zookeeper.ZkClient;
import com.bloomreach.bstore.highavailability.models.ReplicationDiagnostics;
import com.bloomreach.bstore.highavailability.models.SolrCore;
import com.bloomreach.bstore.highavailability.models.ZkBridgeData;
import com.bloomreach.bstore.highavailability.zookeeper.ZkClusterData;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.codehaus.plexus.util.ExceptionUtils;

import java.io.IOException;
import java.util.*;


/**
 * Given a source, destionation zookeeper and a list of collections,
 * it replicates the data from the source cluster to the destination cluster
 * @author nitin
 * @since 1/28/14.
 */
public class CloneCollectionsAction extends SolrFaultTolerantAction {

  private final SourceDestCollectionMapper collectionMapper;
  private ZkClient destinationZKClient;

  /**
   * Init CloneCollectionsAction based on the current Config
   * @param config
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  public CloneCollectionsAction(OperationConfig config) throws IOException, InterruptedException, KeeperException, CollectionPatternException, CollectionNotFoundException {
    super(config);
    String collectionNameRule = this.config.getCollectionNameRule();
    String configNamePatterns = this.config.getConfigNamePatterns();
    collectionMapper = new SourceDestCollectionMapper(collectionNameRule, configNamePatterns, sourceZKClient.getZkClusterData());
    destinationZKClient = new ZkClient(config.getDestinationZkHost());
  }

  @Override
  public void executeAction() throws Exception {
    List<String> operationalCollections = config.getCollections();

    if (operationalCollections.size() == 0) {
      throw new EmptyCollectionListException("No collections specified.. Quitting now");
    }

    Collection<String> collectionNames = findClonableCollectionSet(operationalCollections);

    //Setup Destination Cluster for the collections of interest
    setupDestionationSolrCluster(collectionNames);

    //Do sanity checks before collection can be streamed
    validateClusterSetup(collectionNames);

    //Heart of the computation. Find out the master slave relationship between destination solrcloud cluster
    //and source solrcloud cluster. For every core, a master is picked from source and a slave in its destination
    ZkBridgeData bridgeData = sourceZKClient.matchSourceToDestinationSolrCollectionPeers(destinationZKClient.getZkClusterData(), collectionMapper, collectionNames);

    //Find out the best source of data for every core in the source cluster
    ZkClusterData sourceZkClusterData = sourceZKClient.coreToBestCoreMapping(collectionNames);

    Map<String, SolrCore> srcCoreToBestCoreMapping = sourceZkClusterData.getCoreToBestReplicaMapping();
    for (String coreName : srcCoreToBestCoreMapping.keySet()) {
      logger.info("Best Source Core : " + coreName + " is " + srcCoreToBestCoreMapping.get(coreName).host + ":" + srcCoreToBestCoreMapping.get(coreName).name);
    }

    //Stream data from one cluster to another using Replication Manager and monitor replication Requests
    ReplicatorConfig replicatorConfig = new ReplicatorConfig(bridgeData, collectionNames, collectionMapper, sourceZKClient, config.isSkipReplicationFailures());

    ReplicationManger replicationManger = new ReplicationManger(replicatorConfig);
    replicationManger.replicateCollections();

    List<ReplicationDiagnostics> allDiagnostics = replicationManger.getAllDiagnostics();

    logger.info("Replication Diagnostics Information...");

    for (ReplicationDiagnostics diagnostic : allDiagnostics) {
      if (diagnostic.isFailedReplication()) {
        logger.info("Failed Core:" + diagnostic.getEntity());
        logger.info("Reason:" + diagnostic.getReason());
      } else {
        logger.info("Core :" + diagnostic.getEntity() + ". Time Elapsed " +
                "-> " + diagnostic.getTimeElapsed() + ". Percentage ->" + diagnostic.getPercentageComplete());
      }
    }
  }

  /**
   * If "all" is being passed as the collections, it assumes all the collections are needed.
   * However we give the ability to filter out collections based on any criteria
   * If streamFilter is passed, it allows collections only based on that.
   * If exclusionFilter is passed, it allows collections excluding that.
   * It filters out collection1 by default
   *
   * @param operationalCollections
   * @return {@link Collection<String>} of collections fitting the criteria
   * @return
   */
  private Collection<String> findClonableCollectionSet(List<String> operationalCollections) {
    Collection<String> collectionNames;
    if (operationalCollections.get(0).equalsIgnoreCase("all")) {

      Collection<String> finalCollectionNames = new ArrayList<String>();
      // Build all  collections need to be streamed to destination
      collectionNames = sourceZKClient.getZkClusterData().getCollections();
      collectionNames.remove("collection1");
      boolean dirty = false;
      for (String collName : collectionNames) {
        if (StringUtils.isNotBlank(config.getStreamFilter()) || StringUtils.isNotBlank(config.getExclusionPattern())) {
          dirty = true;
        }
        //Add all collections matching a certain pattern
        if (StringUtils.isNotBlank(config.getStreamFilter()) && collName.contains(config.getStreamFilter())) {
          logger.info("Adding " + collName + " since it matches  pattern " + config.getStreamFilter());
          finalCollectionNames.add(collName);
        }

        //Remove all collections matching Exclusion Pattern
        if (StringUtils.isNotBlank(config.getExclusionPattern()) && collName.contains(config.getExclusionPattern())) {
          logger.info("Skipping " + collName + " since it matches exclusion pattern " + config.getExclusionPattern());
          finalCollectionNames.remove(collName);
        }

      }
      if (dirty) {
        collectionNames = finalCollectionNames;
      }
    } else {
      collectionNames = operationalCollections;
    }
    return collectionNames;
  }


  /**
   * Given a list of collections, source and destination Zk , Setups Solr Collections on the destination cluster
   *
   * @param collectionNames
   * @return
   */
  private boolean setupDestionationSolrCluster(Collection<String> collectionNames) throws Exception {
    // Validate that both zookeepers are in valid states
    logger.info("Received following collections from source zk... ");

    for (String coll : collectionNames) {
      logger.info(coll);
    }
    ZkClusterData destinationZkClusterData = destinationZKClient.getZkClusterData();
    ZkClusterData sourceZkClusterData = sourceZKClient.getZkClusterData();

    boolean destDirty = false;
    for (String collectionName : collectionNames) {

      logger.info("Setting up the  following collections on  destination zk... ");
      checkIfCollectionExistsinSource(sourceZkClusterData, collectionName);

      //Check if destinations Zk Cluster
      String destCollectionName = collectionMapper.dest(collectionName);
      logger.info(destCollectionName);

      //Check if destination doesnt have the collections, then create it
      if (!destinationZkClusterData.getCollections().contains(destCollectionName)) {
        logger.info("Destination zookeeper does not have collection:" + collectionName);
        int desiredNumShards = 1;

        //Fetch the desired num shards from the source cluster.
        if (sourceZkClusterData.getCollections().contains(collectionName)) {
          desiredNumShards = sourceZKClient.getCollectionShards(collectionName);
        }
        logger.info("Creating dest collection: " + destCollectionName + " with " + desiredNumShards + " shards");

        //Fetch the replication Factor from the parameter passed.
        int replicationFactor = Integer.parseInt(config.getReplicationFactor());

        Set<String> destHosts = destinationZkClusterData.getSolrHosts();
        try {
          SolrInteractionUtils.createCollection(destHosts, destCollectionName, collectionMapper.config(collectionName), desiredNumShards, replicationFactor, 500);

        } catch (Exception e) {
          logger.info("Exception creating Collection " + destCollectionName);
          logger.info(ExceptionUtils.getFullStackTrace(e));
          throw new CollectionCreationException(ExceptionUtils.getFullStackTrace(e));
        }
        destDirty = true;
        logger.info("Waiting " + SolrInteractionUtils.DEFAULT_SLEEP_TIME + " seconds before creating the next collection...");
        Thread.sleep(SolrInteractionUtils.DEFAULT_SLEEP_TIME);
      }

    }
    destinationZKClient.refreshZookeeperData();

    return destDirty;
  }

  /**
   * Check if collection exists on the source cluster based on the source zk Cluster Data.
   * @param sourceZkClusterData
   * @param collectionName
   */
  private void checkIfCollectionExistsinSource(ZkClusterData sourceZkClusterData, String collectionName) throws CollectionNotFoundException {
    //Ensure that the source Collection exists
    if (!sourceZkClusterData.getCollections().contains(collectionName)) {
      throw new CollectionNotFoundException("ERROR: source zookeeper does not have collection: " + collectionName + " ... SourceZK: " +
              sourceZKClient.getZkHost());
    }
  }

  /**
   * Checks if the cluster setup(collection shards) at source and destination are valid
   *
   * @param collectionNames
   * @return
   */
  private void validateClusterSetup(Collection<String> collectionNames) throws InvalidClusterSetupAction {
    for (String collectionName : collectionNames) {
      int numSourceShards = sourceZKClient.getCollectionShards(collectionName);
      String destCollectionName = collectionMapper.dest(collectionName);
      int numDestShards = destinationZKClient.getCollectionShards(destCollectionName);

      if (numSourceShards != numDestShards) {
        logger.info("Number of shards in source and destination do not match for collection:" + collectionName + " dest:" + destCollectionName + " sourceShards: " + numSourceShards + " destShards:" + numDestShards);
        throw new InvalidClusterSetupAction("Number of shards in source and destination do not match for collection:" +
                collectionName + " dest:" + destCollectionName +
                " sourceShards= " + numSourceShards + " destShards=" + numDestShards);
      }
    }
  }
}
