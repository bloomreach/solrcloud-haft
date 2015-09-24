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

import com.bloomreach.bstore.highavailability.utils.SolrInteractionUtils;
import com.bloomreach.bstore.highavailability.zookeeper.ZkClient;
import com.bloomreach.bstore.highavailability.models.SolrCore;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Smoke Tests all the collections in a cluster for health and data validations
 * @author nitin
 * @since 4/14/14.
 */
public class SmokeTestClusterAction extends SolrFaultTolerantAction {
  private ZkClient destinationZKClient;

  /**
   * Initialize a Smoke Test Cluster Action
   * @param config
   * @throws Exception
   */
  public SmokeTestClusterAction(OperationConfig config) throws Exception {
    super(config);
    destinationZKClient = new ZkClient(config.getDestinationZkHost());
  }

  @Override
  public boolean executeAction() throws Exception {

    LinkedHashMap<String, String> destionationAliases = destinationZKClient.getZkClusterData().getAliases();
    String destinationSolrHost = destinationZKClient.getZkClusterData().getSolrHosts().toArray()[0].toString();
    String sourceSolrHost = sourceZKClient.getZkClusterData().getSolrHosts().toArray()[0].toString();

    logger.info("Data Validity for the cluster....");
    for (String alias : destionationAliases.keySet()) {
      int destinationSize = SolrInteractionUtils.fetchCollectionSize(alias, destinationSolrHost);

      int sourceSize = 0;
      try {
        sourceSize = SolrInteractionUtils.fetchCollectionSize(alias, sourceSolrHost);
      } catch (Exception e) {
        logger.info("Collection " + alias + "does not exist in source cluster..Skipping");
      }
      if (destinationSize != sourceSize) {
        //Not throwing an exception, since it is just a smoke test report. The user can determine what to do
        //with this information
        logger.info("Collection " + alias + " has mismatching # of documents. Live has " + sourceSize + ". Dest has " + destinationSize);
      }

    }


    Collection<String> allCollections = destinationZKClient.getZkClusterData().getCollections();
    destinationZKClient.fetchClusterHealth(allCollections);
    Set<SolrCore> coresStatus = destinationZKClient.getZkClusterData().getCluterCoresStatus();
    logger.info("Health Check for the cluster...");

    boolean isClusterHealthy = true;
    for (SolrCore individualCore : coresStatus) {
      if (!individualCore.available) {
        //Not throwing an exception, since it is just a smoke test report. The user can determine what to do
        //with this information
        logger.info("Core " + individualCore + " is unavailable..");
        isClusterHealthy = false;
      }
    }
    if (isClusterHealthy) {
      logger.info("Cluster is healthy...");
    }

    return true;
  }
}
