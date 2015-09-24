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
package com.bloomreach.bstore.highavailability.utils;

import com.bloomreach.bstore.highavailability.models.SolrCore;
import com.bloomreach.bstore.highavailability.zookeeper.ZkClusterData;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.ExceptionUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility Class for providing functions that extract and supply information about
 * various solr cores. This includes but not limited to shard mapping, best replica for a core etc
 *
 * @author nitin
 * @since 10/23/14.
 */
public class SolrCoreMapper {
  protected static final Logger logger = Logger.getLogger(SolrCoreMapper.class);

  private ZkClusterData zkClusterData;
  private Collection<String> collectionNames;
  private Map<String, SolrCore> coreToBestReplicaMappingByHealth;
  private Map<String, Map<String, SolrCore>> shardToBestReplicaMapping;


  /**
   * Given a {@link com.bloomreach.bstore.highavailability.zookeeper.ZkClusterData} and a bunch of collections
   * computes core mapping for all cores.
   * @param zkClusterData
   * @param collectionNames
   */
  public SolrCoreMapper(ZkClusterData zkClusterData, Collection<String> collectionNames) {
    this.zkClusterData = zkClusterData;
    this.collectionNames = collectionNames;
    coreToBestReplicaMappingByHealth = new HashMap<String, SolrCore>();
    shardToBestReplicaMapping = new HashMap<String, Map<String, SolrCore>>();
    preComputeBestReplicaMapping();
  }


  /**
   * For all the collections in zookeeper, compute the best replica
   * for every shard for every collection. Doing this computation at bootup
   * significantly reduces the computation done during streaming.
   */
  public void preComputeBestReplicaMapping() {
    Map<String, Map<String, Map<String, String>>> collectionToShardToCoreMapping = getZkClusterData().getCollectionToShardToCoreMapping();

    for (String collection : collectionNames) {
      Map<String, Map<String, String>> shardToCoreMapping = collectionToShardToCoreMapping.get(collection);

      for (String shard : shardToCoreMapping.keySet()) {
        Map<String, String> coreToNodeMap = shardToCoreMapping.get(shard);

        for (String core : coreToNodeMap.keySet()) {
          String currentCore = core;
          String node = coreToNodeMap.get(core);
          SolrCore currentReplica = new SolrCore(node, currentCore);
          try {
            currentReplica.loadStatus();
            //Ok this replica is the best. Let us just use that for all the cores
            fillUpAllCoresForShard(currentReplica, coreToNodeMap);
            break;
          } catch (Exception e) {
            logger.info(ExceptionUtils.getFullStackTrace(e));
            continue;
          }
        }
        shardToBestReplicaMapping.put(shard, coreToBestReplicaMappingByHealth);
      }

    }
  }


  /**
   * Given a core to Node Mapping, Map all the cores for this shard to the best replica
   *
   * @param bestReplica
   * @param coreToNodeMap
   */
  public void fillUpAllCoresForShard(SolrCore bestReplica, Map<String, String> coreToNodeMap) {
    for (String core : coreToNodeMap.keySet()) {
      String currentCore = core;
      logger.info("Using " + bestReplica.name + " for " + currentCore);
      coreToBestReplicaMappingByHealth.put(currentCore, bestReplica);
    }
  }

  /**
   * Given a SolrCore,fetch the Best Core by Health for the current Replica
   *
   * @param core
   * @return
   * @throws Exception
   */
  public SolrCore fetchBestCoreByHealthForReplica(String core) {
    return coreToBestReplicaMappingByHealth.get(core);
  }


  public Map<String, SolrCore> getCoreToBestReplicaMappingByHealth() {
    return coreToBestReplicaMappingByHealth;
  }

  public void setCoreToBestReplicaMappingByHealth(Map<String, SolrCore> coreToBestReplicaMappingByHealth) {
    this.coreToBestReplicaMappingByHealth = coreToBestReplicaMappingByHealth;
  }

  public Map<String, Map<String, SolrCore>> getShardToBestReplicaMapping() {
    return shardToBestReplicaMapping;
  }

  public void setShardToBestReplicaMapping(Map<String, Map<String, SolrCore>> shardToBestReplicaMapping) {
    this.shardToBestReplicaMapping = shardToBestReplicaMapping;
  }

  public ZkClusterData getZkClusterData() {
    return zkClusterData;
  }

  public void setZkClusterData(ZkClusterData zkClusterData) {
    this.zkClusterData = zkClusterData;
  }


}
