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

import cascading.util.Pair;
import com.bloomreach.bstore.highavailability.exceptions.ReplicationFailureException;
import com.bloomreach.bstore.highavailability.utils.SolrInteractionUtils;
import com.bloomreach.bstore.highavailability.models.CorePairMetadata;
import com.bloomreach.bstore.highavailability.models.ReplicationDiagnostics;
import com.bloomreach.bstore.highavailability.models.SolrCore;
import com.bloomreach.bstore.highavailability.models.ZkBridgeData;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Manages all replication related operations and monitors results
 * @author nitin
 * @since 4/11/14.
 */
public class ReplicationManger {
  protected static final Logger logger = Logger.getLogger(ReplicationManger.class);
  private ReplicatorConfig replicatorConfig;
  private List<ReplicationDiagnostics> allDiagnostics;
  private List<Pair<SolrCore, SolrCore>> replicationRequests;

  /**
   * Initializes the Replication Manager.
   * @param config
   */
  public ReplicationManger(ReplicatorConfig config) {
    this.replicatorConfig = config;
    replicationRequests = new ArrayList<Pair<SolrCore, SolrCore>>();
  }


  /**
   * Given a bridge zk data (cross cluster metadata for zookeepers) and collections, replicate the index
   * from a reliable replica on the source cluster to the destination cluster
   * </p>
   *
   * It iterates over the collections from the {@link com.bloomreach.bstore.highavailability.actions.ReplicatorConfig}
   * and for every destination core (in the destination cluster) find the corresponding best source from the source cluster
   * and issue the internal solr replication command. This method collects all such replication commands that are later monitored
   * for completion.
   *
   * @return {@link List<Pair<SolrCore,SolrCore>>>} replication Requests that can be monitored.
   * @throws Exception
   */
  public List<Pair<SolrCore, SolrCore>> replicateCollections() throws Exception {
    allDiagnostics = new ArrayList<ReplicationDiagnostics>();
    Collection<String> collectionNames = replicatorConfig.getCollectionNames();
    ZkBridgeData bridgeData = replicatorConfig.getBridgeData();

    for (String sourceCollectionName : collectionNames) {
      //Fetch the corresponding destination Collection Name from CollectionMapper
      String destinationCollectionName = replicatorConfig.getMapper().dest(sourceCollectionName);

      //Find out the peers corresponding to the destination collection
      Map<String, Pair<CorePairMetadata, CorePairMetadata>> coreToPeersMap = bridgeData.getSourceToDestionationPeerMapping().get(destinationCollectionName);

      logger.debug("Now Streaming: dst: " + destinationCollectionName + " from src: " + sourceCollectionName);
      if (coreToPeersMap == null) {
        throw new ReplicationFailureException("coreToPeersMap is null for : " + destinationCollectionName + " - " + sourceCollectionName);
      }

      //Obtain the best replica for every core for the source Cluster
      Map<String, SolrCore> srcCoreToBestCoreMapping = replicatorConfig.getSourceZKClient().getZkClusterData().getCoreToBestReplicaMapping();

      //The core replication logic. For every destination core, identify the destination core and source core
      for (String destCore : coreToPeersMap.keySet()) {

        //For the given destination core,  fetch the source and destination Core Information
        Pair<CorePairMetadata, CorePairMetadata> pairs = coreToPeersMap.get(destCore);
        //Extract Destination Core and Host Information
        CorePairMetadata destinationCorePairMetadata = pairs.getLhs();
        String destNode = destinationCorePairMetadata.getHostName();
        String destHostName = destNode.split(":")[0];
        SolrCore destinationCore = new SolrCore(destHostName, destCore);

        //Extract Source Core and Host Information
        CorePairMetadata sourceCorePairMetadata = pairs.getRhs();
        String srcCore = sourceCorePairMetadata.getCoreName();
        SolrCore bestsrcCore = srcCoreToBestCoreMapping.get(srcCore);

        //Validate if the Source Core is healthy. Else bomb out
        if (!isSourceCoreHealthy(bestsrcCore)) {
          throw new ReplicationFailureException("Replication Failed since source Core is null or not available.."
                  + ((bestsrcCore == null) ? "source core is null " : bestsrcCore.name));
        } else {
          //Source Core is healthy, Issue the Replication Request
          Pair<SolrCore, SolrCore> replicationRequestPair = replicateToDestination(bestsrcCore, destinationCore);
          if (replicationRequestPair != null) {
            replicationRequests.add(replicationRequestPair);
          }
        }
      }

    }

    return replicationRequests;
  }

  /**
   * Given source and Destionation Cores, Issue the Solr Replication Command between them
   *
   * @param bestsrcCore
   * @param destinationCore
   * @return {@link cascading.util.Pair<SolrCore,SolrCore>} of replication requests.
   * @throws Exception
   */
  public Pair<SolrCore, SolrCore> replicateToDestination(SolrCore bestsrcCore, SolrCore destinationCore) throws Exception {
    //We are good. Replicate to the new cluster
    Pair<SolrCore, SolrCore> replicationRequestPair = null;
    try {
      SolrInteractionUtils.replicateIndex(destinationCore.host, destinationCore.name, bestsrcCore.host, bestsrcCore.name);
      String fullReplication = "http://%s:%s/solr/%s/replication?command=fetchindex&masterUrl=http://%s:%s/solr/%s";
      String replicator = String.format(fullReplication, destinationCore.host,  SolrInteractionUtils.DEFAULT_SOLR_PORT, destinationCore.name, bestsrcCore.host, SolrInteractionUtils.DEFAULT_SOLR_PORT, bestsrcCore.name);
      logger.info("Using Replication Command -> " + replicator);
      replicationRequestPair = new Pair<SolrCore, SolrCore>(destinationCore, bestsrcCore);
      logger.info("Adding Replication Request to the Queue: " + destinationCore.host + ":" + destinationCore.name + " --> " + bestsrcCore.host + ":" + bestsrcCore.name);
    } catch (Exception e) {
      logger.info("Encountered Exception while Trying to connect to destination host " + destinationCore.host + " or source host" + bestsrcCore.host);
      if (!replicatorConfig.shouldSkipReplicationFailures()) {
        throw new ReplicationFailureException("Replication failures while tyring to talk to nodes " + destinationCore.host + " or source host" + bestsrcCore.host);
      }
    }

    return replicationRequestPair;
  }

  /**
   * Given a core, verify if it is healthy for replication
   *
   * @param bestsrcCore
   * @return true if the source core is healthy
   */
  public boolean isSourceCoreHealthy(SolrCore bestsrcCore) {
    //Validate if the Source Core is healthy. Else bomb out
    if (bestsrcCore == null) {
      ReplicationDiagnostics replicatedCoreDiagnostic = new ReplicationDiagnostics();
      replicatedCoreDiagnostic.setEntity("NULL");
      replicatedCoreDiagnostic.setFailedReplication(true);
      replicatedCoreDiagnostic.setReason("Replication Failed since source Core is NULL..");
      allDiagnostics.add(replicatedCoreDiagnostic);
      if (!replicatorConfig.shouldSkipReplicationFailures()) {
        return false;
      }
    } else if (!bestsrcCore.available) {
      //Core is unavailable. Log and error and continue....
      ReplicationDiagnostics replicatedCoreDiagnostic = new ReplicationDiagnostics();
      replicatedCoreDiagnostic.setEntity(bestsrcCore.name);
      replicatedCoreDiagnostic.setFailedReplication(true);
      replicatedCoreDiagnostic.setReason("Replication Failed since source Core is not available..");
      allDiagnostics.add(replicatedCoreDiagnostic);
      if (!replicatorConfig.shouldSkipReplicationFailures()) {
        return false;
      }
    }
    return true;
  }


  /**
   * Given a replication Request, figure out if it is successful. Else add diagnostic info
   *
   * @param request
   * @param diagnostics
   * @return
   */
  public void checkReplicationDataSanity(Pair<SolrCore, SolrCore> request, ReplicationDiagnostics diagnostics) throws Exception {
    SolrCore replicatedCore = request.getLhs();
    for (int retry = 0; retry < 10; retry++) {
      try {
        replicatedCore.loadStatus();
        logger.info("Obtained Core Status successfully " + replicatedCore);
        break;
      } catch (Exception e) {
        logger.info("Encountered Exception while loading core status " + replicatedCore + " Retrying #" + retry);
        if (retry == 9) {
          if (isCollectionQueryable(replicatedCore)) {
            logger.info("Unable to load core status but able to query core succesfully:" + replicatedCore.name);
            return;
          }
        }
        Thread.sleep(SolrInteractionUtils.DEFAULT_SLEEP_TIME);
      }
    }

    if (!isSourceCoreHealthy(replicatedCore)) {
      if (!replicatorConfig.shouldSkipReplicationFailures()) {
        throw new ReplicationFailureException("Replication Failed due to Replicated Core being unavailable.." + replicatedCore.name);
      }
    } else {
      SolrCore bestsrcCore = request.getRhs();
      Pair<Boolean, String> replicationSuccessToReasonPair = bestsrcCore.dataMatch(replicatedCore);
      if (!(replicationSuccessToReasonPair.getLhs())) {
        diagnostics.setEntity(replicatedCore.name);
        diagnostics.setReason(replicationSuccessToReasonPair.getRhs());
        diagnostics.setFailedReplication(true);
        logger.info("Replication Failed: " + diagnostics.getEntity() + " " + diagnostics.getReason());
        allDiagnostics.add(diagnostics);
        if (!replicatorConfig.shouldSkipReplicationFailures()) {
          throw new ReplicationFailureException("Error Replication Data in core " + replicatedCore.name + " . Reason is " + replicationSuccessToReasonPair.getRhs());
        }
      } else {
        logger.info("Replication Data match succeeded for  " + replicatedCore.name);
      }
    }

  }

  /**
   * Check if collection is queryable
   */
  private boolean isCollectionQueryable(SolrCore core) throws ReplicationFailureException {
    try {
      int size = SolrInteractionUtils.fetchCollectionSize(core.name, core.host);
      logger.info("Collection size is " + size);
      return true;
    } catch (Exception e) {
      throw new ReplicationFailureException("Cannot query collection size");
    }
  }

  /**
   * Given a list of Replication Requests, monitor and report on their status.
   * It uses the {@link #replicationRequests} instance variable to iterate and track the requests.
   *
   * @return boolean if all replication requests succeed.
   * @throws Exception
   */
  public boolean monitorReplicationRequests() throws Exception {
    logger.info("Waiting for replication Tasks to complete...");
    long totalTime = 0;
    long maxTime = 20 * 60 * 1000;
    boolean replicationFailed = false;
    //Monitor Replication Requests by verifying Status, Health and Data Sanity of all cores
    for (Pair<SolrCore, SolrCore> request : replicationRequests) {
      ReplicationDiagnostics diagnostics;

      try {
        diagnostics = checkIfReplicationComplete(request, totalTime, maxTime);
      } catch (Exception e) {
        logger.info(ExceptionUtils.getFullStackTrace(e));
        throw new ReplicationFailureException(ExceptionUtils.getFullStackTrace(e));
      }

      logger.info("After Replication the core takes a while to reflect the right status...Waiting for 2 seconds");
      //Check if Replication went through and data is in parity between source and destination
      try {
        checkReplicationDataSanity(request, diagnostics);
      } catch (Exception e) {
        logger.info(ExceptionUtils.getFullStackTrace(e));
        throw new ReplicationFailureException(ExceptionUtils.getFullStackTrace(e));
      }

    }

    //Check if we reached a max time.
    if (totalTime >= maxTime) {
      ReplicationDiagnostics replicatedCoreDiagnostic = new ReplicationDiagnostics();
      replicatedCoreDiagnostic.setEntity("all");
      replicatedCoreDiagnostic.setReason("Replication timed out: timeSpent[\" + totalTime + \"] >= max[\" + maxTime + \"]");
      allDiagnostics.add(replicatedCoreDiagnostic);
      if (!replicatorConfig.shouldSkipReplicationFailures()) {
        throw new ReplicationFailureException("Replication timed out: timeSpent[" + totalTime + "] >= max[" + maxTime + "]");
      }
    }
    logger.info("Completed Monitoring all replication Requests...");

    return replicationFailed;
  }

  /**
   * Given a Replication Request, check (with retries) if it is successful.
   *
   * @param request
   * @param totalTime
   * @param maxTime
   * @return {@link com.bloomreach.bstore.highavailability.models.ReplicationDiagnostics} for a given replication request.
   * @throws Exception
   */
  public ReplicationDiagnostics checkIfReplicationComplete(Pair<SolrCore, SolrCore> request, long totalTime, long maxTime) throws Exception {
    String coreName = request.getLhs().name;
    String hostName = request.getLhs().host;
    //TODO: Use Generic Retryer here
    ReplicationDiagnostics diagnostics = SolrInteractionUtils.checkReplicationStatus(hostName, coreName);
    while ((diagnostics.isReplicating()) && (totalTime < maxTime)) {
      logger.info("Core " + coreName + " is still replication.. Waiting for 2 seconds before checking the status again...");
      diagnostics = SolrInteractionUtils.checkReplicationStatus(hostName, coreName);
      logger.info("Waiting for: " + diagnostics.getEntity() + " " + diagnostics.getPercentageComplete());
      Thread.sleep(SolrInteractionUtils.DEFAULT_SLEEP_TIME);
      totalTime += 2000;
      diagnostics = SolrInteractionUtils.checkReplicationStatus(hostName, coreName);
    }
    logger.info("Core " + coreName + " has completed Replication. Moving onto core health checks for " + coreName);
    return diagnostics;
  }

  public List<ReplicationDiagnostics> getAllDiagnostics() {
    return allDiagnostics;
  }

}
