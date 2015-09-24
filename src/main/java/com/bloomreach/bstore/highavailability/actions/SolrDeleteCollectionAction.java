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

import java.util.Arrays;
import java.util.List;

/**
 * Deletes Solr Collection
 * @author nitin
 * @since 3/6/14.
 */
public class SolrDeleteCollectionAction extends SolrFaultTolerantAction {

  public SolrDeleteCollectionAction(OperationConfig config) throws Exception {
    super(config);
  }

  @Override
  public boolean executeAction() throws Exception {
    String solrHost = (String) sourceZKClient.getZkClusterData().getSolrHosts().toArray()[0];

    List<String> collections;

    if (config.getCollections().size() == 0) {
      logger.info("No collections passed.. Returning");
      return true;
    } else {
      collections = config.getCollections();
    }
    String logMessage = "Deleting collections %s on %s";
    logger.info(String.format(logMessage, Arrays.toString(collections.toArray()), solrHost));

    boolean succesfulDelete = true;
    //Delete the collection
    for (String collectionName : collections) {
      if (collectionName.equals("collection1")) {
        //skip the default collection
        logger.info("Cannot Delete Collection1. Do you want to be on pager?");
        continue;
      }
      try {
        SolrInteractionUtils.deleteCollection(solrHost, collectionName);
      } catch (Exception e) {
        logger.info("Delete collection failed for " + collectionName);
        succesfulDelete = false;
      }
    }

    return succesfulDelete;
  }
}
