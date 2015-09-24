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

import cascading.util.Pair;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.bloomreach.bstore.highavailability.actions.SourceDestCollectionMapper;
import com.bloomreach.bstore.highavailability.decorators.ZkCollectionState;
import com.bloomreach.bstore.highavailability.decorators.ZkReplicaInfo;
import com.bloomreach.bstore.highavailability.decorators.ZkShardInfo;
import com.bloomreach.bstore.highavailability.exceptions.CollectionNotFoundException;
import com.bloomreach.bstore.highavailability.models.CorePairMetadata;
import com.bloomreach.bstore.highavailability.models.SolrCore;
import com.bloomreach.bstore.highavailability.models.ZkBridgeData;
import com.bloomreach.bstore.highavailability.utils.AwsConfigReader;
import com.bloomreach.bstore.highavailability.utils.SolrCoreMapper;
import com.bloomreach.bstore.highavailability.utils.SolrInteractionUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.*;

/**
 * Client to  encapsulate {@link org.apache.zookeeper} access,data transformations and refresh Zookeeper cluster data.
 * </p>
 * <p/>
 * It also provides a view of the zk cluster data to the given {@link org.apache.zookeeper}  and avoids any
 * global state sharing.
 * </p>
 * <p/>
 * It has the ability to work off both private and public ips. It figures out whether you are local or inside
 * a data center and does the translation accordingly. This is checked by {@link #isRunningInsideDataCenter()}
 * </p>
 * <p/>
 * You can run it locally (needs public DNS names)for SolrCloud. To do that you need to provide aws_credentials in
 * aws_credentials.properties files. This will use the {@link com.amazonaws.services.ec2.AmazonEC2}
 * api to do the translation for you. The translation is achieved by the {@link #translatePrivateIpToPublicHostNames()}
 *
 * @author nitin
 * @since 2/18/14.
 */
public class ZkClient {
  private static final Logger logger = Logger.getLogger(ZkClient.class);
  public final static String EMPTY_LEADER = "NO_LEADER";

  /* Source Zk Host */
  private String zkHost;
  /* Zk Handle */
  private ZooKeeper zookeeperHandle;
  /* Indication of local Run*/
  private boolean localRun;
  /* All Solr Nodes */
  private Set<String> allSolrNodes;
  /* Zookeeper Cluster Data */
  private ZkClusterData zkClusterData;
  /* SolrCore Mapper */
  private SolrCoreMapper coreMapperForCluster;


  /**
   * Constructs an in memory view of the SolrCloud Cluster Data from
   * the zookeeper ensemble.
   *
   * @param zkHost The destination Zookeper Host
   */
  public ZkClient(String zkHost) throws IOException, InterruptedException, KeeperException, CollectionNotFoundException {
    this.zkHost = zkHost;
    zkClusterData = refreshZookeeperData();
  }

  public void setLocalRun(boolean localRun) {
    this.localRun = localRun;
  }

  public void setZkClusterData(ZkClusterData zkClusterData) {
    this.zkClusterData = zkClusterData;
  }

  public String getZkHost() {
    return zkHost;
  }

  public void setZkHost(String zkHost) {
    this.zkHost = zkHost;
  }

  public ZooKeeper getZookeeperHandle() {
    return zookeeperHandle;
  }

  public void setZookeeperHandle(ZooKeeper zookeeperHandle) {
    this.zookeeperHandle = zookeeperHandle;
  }


  public SolrCoreMapper getCoreMapperForCluster() {
    return coreMapperForCluster;
  }

  /**
   * Refreshes the zookeeper data by creating a new connection
   * It is used to load the data on Zk sourceZKClient bootup but custom operations
   * can refresh their own view of zookeeper as needed. This isolates
   * the global state of zookeeper.
   *
   * @return
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   */
  public ZkClusterData refreshZookeeperData() throws IOException, InterruptedException, KeeperException, CollectionNotFoundException {
    zookeeperHandle = ZKConnectionManager.connectToZookeeper(this.zkHost);
    setZookeeperHandle(zookeeperHandle);
    zkClusterData = new ZkClusterData();
    setZkClusterData(zkClusterData);
    allSolrNodes = new HashSet<String>();
    try {
      fetchZookeeperClusterState();
      fetchAllConfigs();
      fetchAlias();
      fetchLiveSolrNodes();
      fetchAllSolrNodesForTranslation();
      //Check to decide if it is running outside ec2
      if (!isRunningInsideDataCenter()) {
        setLocalRun(true);
        translatePrivateIpToPublicHostNames();
      }
      fetchAllCollections();
      fetchCollectionToNodeMapping();
      fetchNodeToCoreMapping();
      fetchNodeToCoreToStatusMapping();
      fetchAllCollectionMetadata();
      fetchLeaderStatus();
      fetchCollectionToShardToCoreMapping();
    } finally {
      if (getZookeeperHandle() != null) {
        getZookeeperHandle().close();
      }
    }
    return getZkClusterData();
  }


  /**
   * Given a setup of solr hosts, it tries to query solr running on them using the private ip.
   * If they return a valid response, we are running inside ec2 realm
   * else we need to run in local mode.
   *
   * @return boolean if it is running inside a DC or not
   */
  private boolean isRunningInsideDataCenter() {
    Set<String> hosts = zkClusterData.getSolrHosts();
    String solrPingQueryTemplate = "http://%s:%s/solr/collection1/select?q=*:*";
    for (String host : hosts) {
      String solrQuery = String.format(solrPingQueryTemplate, host, SolrInteractionUtils.DEFAULT_SOLR_PORT );
      try {
        SolrInteractionUtils.executeSolrCommandWithTimeout(SolrInteractionUtils.CONNECT_TIMEOUT,solrQuery );
        logger.info("Succesfully connected to host " + host + ". We can use Internal Ips..");
        return true;
      } catch (IOException e) {
        logger.info("Encountered Exception while trying to connect to " + host);
        return false;
      }
    }
    logger.info("Connecting to all the hosts with Internal Ips failed. Running in local mode..");
    return false;
  }

  /**
   * Fetches the  /clusterstate.json and transforms
   * to a POJO using Jackson. Check {@link com.bloomreach.bstore.highavailability.decorators.ZkCollectionState}
   * for the uber decorator object.
   *
   * @return {@link #getZkClusterData()}
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonParseException
   */
  public ZkClusterData fetchZookeeperClusterState() throws KeeperException, InterruptedException, JsonParseException, JsonMappingException, IOException {
    Stat zkNodeStat = new Stat();
    byte[] clusterStateBytes = getZookeeperHandle().getData("/clusterstate.json", null, zkNodeStat);
    String clusterState = new String(clusterStateBytes);

    ObjectMapper mapper = new ObjectMapper();
        /* Data from ZooKeeper cluster state would look as follows
       "nitin_collection":{
        "shards":{
            "shard1":{
                "range":"80000000-9998ffff",
                        "state":"active",
                        "replicas":{
                    "10.225.148.240:8983_solr_nitin_collection_shard1_replica2":{
                        "shard":"shard1",
                                "state":"active",
                                "core":"nitin_collection_shard1_replica2",
                                "collection":"nitin_collection",
                                "node_name":"10.225.148.240:8983_solr",
                                "base_url":"http://10.225.148.240:8983/solr"},
                    "10.218.149.162:8983_solr_nitin_collection_shard1_replica1":{
                        "shard":"shard1",
                                "state":"active",
                                "core":"nitin_collection_shard1_replica1",
                                "collection":"nitin_collection",
                                "node_name":"10.218.149.162:8983_solr",
                                "base_url":"http://10.218.149.162:8983/solr",
                                "leader":"true"}}},
         */
    LinkedHashMap<String, ZkCollectionState> state = mapper.readValue(clusterState, new TypeReference<LinkedHashMap<String, ZkCollectionState>>() {
    });
    //size of Cluster State Json File
    long dataLength = zkNodeStat.getDataLength();
    getZkClusterData().setClusterStateSize(dataLength);
    logger.info("Data Size for Cluster State File is " + dataLength);
    //Set Cluster State
    getZkClusterData().setClusterState(state);
    return getZkClusterData();
  }


  /**
   * Based on the internal Cluster State, construct the <collection, shard, leader_host> mapping
   * for all collections.
   *
   * @return {@link #getZkClusterData()} with the leader status updated for all collections.
   */
  public ZkClusterData fetchLeaderStatus() {
    //Get Zookeeper Cluster state
    LinkedHashMap<String, ZkCollectionState> zkState = getZkClusterData().getClusterState();

    //Initialize collection to shard to Leader mapping
    Map<String, Map<String, String>> collectionToShardLeaderMapping = new HashMap<String, Map<String, String>>();

    //Zookeeper keys off cluster state by collections
    for (String collection : zkState.keySet()) {

      if (collection.equals("collection1")) {
        continue;
      }
      ZkCollectionState collectionMetadata = zkState.get(collection);
      Map<String, String> shardToLeaderMapping = new HashMap<String, String>();

      //Every collection data internall keys everything by shard id
      for (String shard : collectionMetadata.getShards().keySet()) {
        ZkShardInfo shardInfo = collectionMetadata.getShards().get(shard);
        shardToLeaderMapping.put(shard, EMPTY_LEADER);
        //Every shard id uses replica id as the key for storing node
        for (String replica : shardInfo.getReplicas().keySet()) {
          ZkReplicaInfo replicaInfo = shardInfo.getReplicas().get(replica);

          if (replicaInfo.getLeader() != null) {
            if (replicaInfo.getLeader().equalsIgnoreCase("true")) {
              String nodeNameFull = replicaInfo.getNodeName();
              String coreName = replicaInfo.getCore();
              shardToLeaderMapping.put(shard, String.format("%s,%s", nodeNameFull, coreName));
              break;
            }
          }
        }
      }
      collectionToShardLeaderMapping.put(collection, shardToLeaderMapping);
    }

    for (String collection : collectionToShardLeaderMapping.keySet()) {
      Map<String, String> shardLeaderMap = collectionToShardLeaderMapping.get(collection);
      logger.info("Collection: " + collection);
      for (String shard : shardLeaderMap.keySet()) {
        logger.info("Shard = > " + shard + ". Leader => " + shardLeaderMap.get(shard));
      }
    }
    getZkClusterData().setCollectionToShardLeaderMapping(collectionToShardLeaderMapping);
    return getZkClusterData();
  }

  /**
   * Fetch Alias Information from Zookeeper as {@link java.util.LinkedHashMap<String,String>}
   *
   * @return {@link #getZkClusterData()} with the alias information updated.
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonParseException
   */
  public ZkClusterData fetchAlias() throws KeeperException, InterruptedException, IOException {
    //Fetching Alias Information
    byte[] aliasStateBytes = getZookeeperHandle().getData("/aliases.json", null, null);
    if (aliasStateBytes == null) {
      getZkClusterData().setAliases(null);
    } else {
      String aliases = new String(aliasStateBytes);

      ObjectMapper mapper = new ObjectMapper();
      HashMap<String, Object> alias = mapper.readValue(aliases, HashMap.class);
      LinkedHashMap<String, String> allAlias = (LinkedHashMap<String, String>) alias.get("collection");
      getZkClusterData().setAliases(allAlias);
    }

    return getZkClusterData();
  }

  /**
   * Fetches live SolrCloud Nodes based on the live_nodes directory in zookeeper.
   *
   * @return {@link #getZkClusterData()} with live nodes of solrcloud updated.
   * @throws KeeperException
   * @throws InterruptedException
   */
  public ZkClusterData fetchLiveSolrNodes() throws KeeperException, InterruptedException {

    Stat zkNodeStat = new Stat();
    Set<String> hosts = new HashSet<String>();
    List<String> liveHostPorts = getZookeeperHandle().getChildren("/live_nodes", null, zkNodeStat);
    for (String liveSolrHost : liveHostPorts) {
      String nodePrivateIp = liveSolrHost.split("_solr")[0].split(":")[0];
      hosts.add(nodePrivateIp);
    }

    getZkClusterData().updateSolrNodes(hosts);
    return getZkClusterData();
  }

  /**
   * Populates Collection to Node Mapping. It is {@link Map<String, Map<String, String>>} that
   * maps every <collection_name, core, host>
   *
   * @return {@link #getZkClusterData()} with live nodes of solrcloud updated.
   */
  public ZkClusterData fetchCollectionToNodeMapping() {
    //Get Zookeeper Cluster state
    LinkedHashMap<String, ZkCollectionState> zkState = getZkClusterData().getClusterState();

    Map<String, String> coreToNodeMap = null;
    Map<String, Map<String, String>> collectionToNodeMap = new HashMap<String, Map<String, String>>();

    //Zookeeper keys off cluster state by collections
    for (String collection : zkState.keySet()) {
      ZkCollectionState collectionMetadata = zkState.get(collection);
      coreToNodeMap = new HashMap<String, String>();
      //Every collection data internall keys everything by shard id
      for (String shard : collectionMetadata.getShards().keySet()) {
        ZkShardInfo shardInfo = collectionMetadata.getShards().get(shard);
        //Every shard id uses replica id as the key for storing node
        for (String replica : shardInfo.getReplicas().keySet()) {
          String nodeNameFull = shardInfo.getReplicas().get(replica).getNodeName();
          String nodeName = fetchNodeName(nodeNameFull);
          String core = shardInfo.getReplicas().get(replica).getCore();
          coreToNodeMap.put(core, nodeName);
        }
      }
      collectionToNodeMap.put(collection, coreToNodeMap);
    }
    getZkClusterData().setCollectionToNodeMapping(collectionToNodeMap);
    return getZkClusterData();
  }


  /**
   * Populates Collection to Shard to Core To Node Mapping. It is {@link Map<String, Map<String, Map<String, String>>>} that
   * maps every <collection_name, core, host>
   *
   * @return {@link #getZkClusterData()} with Collection to Shard to Core To Node Mapping.
   */
  public ZkClusterData fetchCollectionToShardToCoreMapping() {
    //Get Zookeeper Cluster state
    LinkedHashMap<String, ZkCollectionState> zkState = getZkClusterData().getClusterState();

    Map<String, String> coreToNodeMap = null;
    Map<String, Map<String, String>> shardToCoreToNodeMap = null;
    Map<String, Map<String, Map<String, String>>> collectionToShardToCoreMapping = new HashMap<String, Map<String, Map<String, String>>>();

    //Zookeeper keys off cluster state by collections
    for (String collection : zkState.keySet()) {
      if (collection.equals("collection1")) {
        continue;
      }
      ZkCollectionState collectionMetadata = zkState.get(collection);
      shardToCoreToNodeMap = new HashMap<String, Map<String, String>>();
      //Every collection data internall keys everything by shard id
      for (String shard : collectionMetadata.getShards().keySet()) {
        ZkShardInfo shardInfo = collectionMetadata.getShards().get(shard);
        coreToNodeMap = new HashMap<String, String>();

        //Every shard id uses replica id as the key for storing node
        for (String replica : shardInfo.getReplicas().keySet()) {
          String nodeNameFull = shardInfo.getReplicas().get(replica).getNodeName();
          String nodeName = fetchNodeName(nodeNameFull);
          String core = shardInfo.getReplicas().get(replica).getCore();
          coreToNodeMap.put(core, nodeName);
        }
        shardToCoreToNodeMap.put(shard, coreToNodeMap);
      }
      collectionToShardToCoreMapping.put(collection, shardToCoreToNodeMap);
    }
    getZkClusterData().setCollectionToShardToCoreMapping(collectionToShardToCoreMapping);
    return getZkClusterData();
  }

  /**
   * Fetch all collections in solrcloud based on clusterstate keys
   *
   * @return {@link #getZkClusterData()} with all Collections.
   */
  public ZkClusterData fetchAllCollections() {
    Set<String> collections = getZkClusterData().getClusterState().keySet();
    getZkClusterData().setCollections(collections);
    return getZkClusterData();
  }

  /**
   * Given a Fully Qualified replica name, get the ip from it.
   *
   * @return
   */
  public String fetchNodeName(String replicaFQName) {
    //Replica Names are mentioned in Solr as <ip>_8983_solr_<collection_shard<num>_replica<num>>
    String nodeName = replicaFQName.split("_solr")[0].split(":")[0];
    if (localRun) {
      //logger.info("Doing Private host to IP Mapping for node " + nodeName);
      //do the ec2 mapping and return full ec2 name
      if (zkClusterData.getPrivateIpToPublicHostNameMap() != null) {
        nodeName = zkClusterData.getPrivateIpToPublicHostNameMap().get(nodeName);
      } else {
        throw new RuntimeException("Trying to do Private to Public host Ip mapping is failed for host " + nodeName);
      }
    }
    return nodeName;
  }

  /**
   * Fetch All Solr Nodes from Cluster State
   */
  private void fetchAllSolrNodesForTranslation() {
    LinkedHashMap<String, ZkCollectionState> zkState = getZkClusterData().getClusterState();
    for (String collection : zkState.keySet()) {
      //Get all shards for a given collection and reverse map them to nodes
      ZkCollectionState collectionMetadata = zkState.get(collection);
      for (String shard : collectionMetadata.getShards().keySet()) {
        ZkShardInfo shardInfo = collectionMetadata.getShards().get(shard);
        for (String replica : shardInfo.getReplicas().keySet()) {
          String nodeNameFull = shardInfo.getReplicas().get(replica).getNodeName();
          String nodeName = nodeNameFull.split("_solr")[0].split(":")[0];
          allSolrNodes.add(nodeName);
        }
      }
    }
  }

  /**
   * In SolrCloud every chunk of data (shard or replica) is internally a core. It can be thought of as a
   * unique identifier across the entire solrcloud cluster. In ZooKeeper cluster state is keyed off by the collection
   * name. The following method constructs a reverse mapping between nodes to their corresponding cores.
   *
   * @return {@link #getZkClusterData()} with Node to Core Mapping.
   */
  public ZkClusterData fetchNodeToCoreMapping() {
    Map<String, List<String>> nodeToCoreMap = new HashMap<String, List<String>>();
    LinkedHashMap<String, ZkCollectionState> zkState = getZkClusterData().getClusterState();
    for (String collection : zkState.keySet()) {
      //Get all shards for a given collection and reverse map them to nodes
      ZkCollectionState collectionMetadata = zkState.get(collection);
      for (String shard : collectionMetadata.getShards().keySet()) {
        ZkShardInfo shardInfo = collectionMetadata.getShards().get(shard);
        for (String replica : shardInfo.getReplicas().keySet()) {
          String nodeNameFull = shardInfo.getReplicas().get(replica).getNodeName();
          String nodeName = fetchNodeName(nodeNameFull);
          String core = shardInfo.getReplicas().get(replica).getCore();
          //Map the actual node name to a list of available cores/replicas
          if (nodeToCoreMap.get(nodeName) != null) {
            List<String> cores = nodeToCoreMap.get(nodeName);
            cores.add(core);
            nodeToCoreMap.put(nodeName, cores);
          } else {
            List<String> newCores = new ArrayList<String>();
            newCores.add(core);
            nodeToCoreMap.put(nodeName, newCores);
          }
        }

      }
    }
    getZkClusterData().setNodeToCoreMapping(nodeToCoreMap);
    return getZkClusterData();
  }


  /**
   * Fetch the num of shards for a given collection
   *
   * @param collectionName Name of the collection
   * @return numShards for the give collection.
   */
  public int getCollectionShards(String collectionName) {
    LinkedHashMap<String, ZkCollectionState> zkState = getZkClusterData().getClusterState();
    if (!zkState.containsKey(collectionName)) {
      logger.error("Zookeeper state does not have collection:" + collectionName);
      return 0;
    }
    ZkCollectionState collectionMetadata = zkState.get(collectionName);
    return collectionMetadata.getShards().size();
  }


  /**
   * Return a view of the zookeeper cluster data.
   *
   * @return {@link com.bloomreach.bstore.highavailability.zookeeper.ZkClusterData}
   */
  public ZkClusterData getZkClusterData() {
    return zkClusterData;
  }


  /**
   * Gets all configs available in zookeeper
   *
   * @return {@link #getZkClusterData()} with all Configs Populated.
   * @throws Exception
   */
  public ZkClusterData fetchAllConfigs() throws KeeperException, InterruptedException {
    List<String> configs = getZookeeperHandle().getChildren("/configs", null, null);
    logger.info("Found the following configs in zookeeper...");
    for (String config : configs) {
      logger.info(config);
    }
    getZkClusterData().setConfigs(configs);
    return getZkClusterData();
  }

  /**
   * Obtains a collection to config Map for all collections
   *
   * @return {@link #getZkClusterData()} with all Collection To Config Metadata Populated.
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonParseException
   */
  public ZkClusterData fetchAllCollectionMetadata() throws KeeperException, InterruptedException, IOException, CollectionNotFoundException {
        /* get /collections/nitin_collection
             {
            "configName":"test_config",
            "router":"implicit"
            }

         */
    Collection<String> collections = getZkClusterData().getCollections();
    ObjectMapper mapper = new ObjectMapper();
    for (String collection : collections) {
      String zkPath = "/collections/" + collection;
      if (getZookeeperHandle().exists(zkPath, false) == null) {
        logger.info("Skipping collection as it is no longer in zk node... " + collection);
        continue;
      }
      byte[] collectionMetadata = getZookeeperHandle().getData(zkPath, null, null);
      if (collectionMetadata == null) {
        throw new CollectionNotFoundException("Can't get data for collection " + collection);
      }
      String collectionMetadataString = new String(collectionMetadata);
      HashMap<String, Object> collectionMetadataOutput = mapper.readValue(collectionMetadataString, HashMap.class);
      String configName = (String) collectionMetadataOutput.get("configName");
      if (getZkClusterData().getCollectionToConfigMapping() == null) {
        getZkClusterData().setCollectionToConfigMapping(new HashMap<String, String>());
      }
      getZkClusterData().getCollectionToConfigMapping().put(collection, configName);

    }
    return getZkClusterData();
  }

  /**
   * Given a source and destination Zookeeper data it computes peer cores across both clusters. This information is needed
   * to map source cores and destination cores to replicate data from one to the other.
   * The details of how a core on source cluster is mapped to a core on the destination cluster is determined by
   * {@link #fetchCoreByReplicaSymmetry}.
   *
   * @param mapper
   * @return
   */
  public ZkBridgeData matchSourceToDestinationSolrCollectionPeers(ZkClusterData destinationZkClusterData, SourceDestCollectionMapper mapper, Collection<String> collections) {
    ZkBridgeData bridgeData = new ZkBridgeData();
    Map<String, Map<String, Pair<CorePairMetadata, CorePairMetadata>>> collectionsToPeerMap = new HashMap<String, Map<String, Pair<CorePairMetadata, CorePairMetadata>>>();

    //Get collection to Node Mappings for the current zk Cluster
    Map<String, Map<String, String>> sourceCollectionToNodeMap = getZkClusterData().getCollectionToNodeMapping();

    //Get collection to Node Mappings for the destination zk Cluster
    Map<String, Map<String, String>> destCollectionToNodeMap = destinationZkClusterData.getCollectionToNodeMapping();
    Map<String, Pair<CorePairMetadata, CorePairMetadata>> solrCorePeerMap = null;

    for (String destCollection : destCollectionToNodeMap.keySet()) {
      if (destCollection.contains("collection1")) {
        continue;
      }


      //Get the desination Collection and identify the corresponding source collection Name (by applying rules from mapper)
      String srcCollection = mapper.src(destCollection);
      if (!collections.contains(srcCollection)) {
        logger.info("Skipping collection " + srcCollection + " as it is not being requested for replication...");
        continue;
      }
      logger.info("Mapping  dest collection:" + destCollection + " to src collection:" + srcCollection);

      //Validate Source Collection to ensure it has the collection in it
      Map<String, String> sourceCoreToNodeMap = sourceCollectionToNodeMap.get(srcCollection);
      if (sourceCoreToNodeMap == null) {
        logger.info("sourceCoreToNodeMap for " + srcCollection + " missing in sourceCollectionToNodeMap: skipping");
        continue;
      }

      Map<String, String> destCoretoNodeMap = destCollectionToNodeMap.get(destCollection);
      solrCorePeerMap = new HashMap<String, Pair<CorePairMetadata, CorePairMetadata>>();

      //For every core in the current collection, identify nodes that have the same core in the source cluster and in the destination cluster
      for (String destinationCore : destCoretoNodeMap.keySet()) {
        String sourceCore = mapper.src(destinationCore);

        //Check Replica Symmetry and fetch Core Name
        sourceCore = fetchCoreByReplicaSymmetry(sourceCoreToNodeMap, sourceCore, destinationCore);

        //Do the actual mapping
        String sourceNode = sourceCoreToNodeMap.get(sourceCore);
        CorePairMetadata sourcePairMetadata = new CorePairMetadata();
        sourcePairMetadata.setCoreName(sourceCore);
        sourcePairMetadata.setHostName(sourceNode);

        String destNode = destCoretoNodeMap.get(destinationCore);
        CorePairMetadata destinationPairMetadata = new CorePairMetadata();
        destinationPairMetadata.setCoreName(destinationCore);
        destinationPairMetadata.setHostName(destNode);
        logger.info("Core Mapping Destination Core " + destinationCore + " on destNode " + destNode + " to  Source Core " + sourceCore + " on Source Node" + sourceNode);
        Pair<CorePairMetadata, CorePairMetadata> pair = new Pair<CorePairMetadata, CorePairMetadata>(destinationPairMetadata, sourcePairMetadata);

        solrCorePeerMap.put(destinationCore, pair);
      }
      collectionsToPeerMap.put(destCollection, solrCorePeerMap);
      logger.info("Put the collection:  " + destCollection);

    }

    bridgeData.setSourceToDestionationPeerMapping(collectionsToPeerMap);
    return bridgeData;
  }


  /**
   * In SolrCloud every chunk of data (shard or replica) is internally a core. It can be thought of as a
   * unique identifier across the entire solrcloud cluster. In ZooKeeper cluster state is keyed off by the collection
   * name. The following method constructs a reverse mapping between nodes to their corresponding cores and its health
   * (up or down).
   *
   * @return {@link #getZkClusterData()} with all Collection To Config Metadata Populated.
   */
  public ZkClusterData fetchNodeToCoreToStatusMapping() {
    Map<String, Map<String, String>> nodetoCoreHealthMap = new HashMap<String, Map<String, String>>();
    LinkedHashMap<String, ZkCollectionState> zkState = getZkClusterData().getClusterState();
    for (String collection : zkState.keySet()) {
      //Get all shards for a given collection and reverse map them to nodes
      ZkCollectionState collectionMetadata = zkState.get(collection);
      for (String shard : collectionMetadata.getShards().keySet()) {
        ZkShardInfo shardInfo = collectionMetadata.getShards().get(shard);
        for (String replica : shardInfo.getReplicas().keySet()) {
          String nodeNameFull = shardInfo.getReplicas().get(replica).getNodeName();
          String nodeName = fetchNodeName(nodeNameFull);
          String core = shardInfo.getReplicas().get(replica).getCore();
          String state = shardInfo.getReplicas().get(replica).getState();
          //Map the actual node name to a list of available cores/replicas
          if (nodetoCoreHealthMap.get(nodeName) != null) {
            Map<String, String> coreToHealth = nodetoCoreHealthMap.get(nodeName);
            coreToHealth.put(core, state);
            nodetoCoreHealthMap.put(nodeName, coreToHealth);
          } else {
            Map<String, String> coreToHealth = new HashMap<String, String>();
            coreToHealth.put(core, state);
            nodetoCoreHealthMap.put(nodeName, coreToHealth);
          }
        }

      }
    }
    getZkClusterData().setNodetoCoreHealthMap(nodetoCoreHealthMap);
    return getZkClusterData();
  }

  /**
   * Given the sourceCore to Node mapping and destination core, fetch the source Core corresponding to it based
   * on Replica Symmetry
   * <p/>
   * If both source and destination have same number of replicas (all replica entires are in the map), then we can return the source Core itself.
   * If certain destination replicas do not have matching source Core Replicas, then we might have to map the only available source
   * replica to all the destination replicas
   * <p/>
   * Example:
   * Source Collection could have shard1_replica1 and destination might have 2 replicas (shard1_replica1, shard2_replica2).
   * This means that we need to still map the one available replica to both the other replicas so that they could be in sync.
   * This is done as follows (Get the shard key <collection>_shard1 and check if replica1 exists in source.
   * If it is available them match all destination replicas for this shard to stream for the only available replica
   *
   * @param sourceCoreToNodeMap Core placement for current cluster nodes
   * @param sourceCore          name of the source core
   * @param destinationCore     destination core
   * @return String
   */
  private String fetchCoreByReplicaSymmetry(Map<String, String> sourceCoreToNodeMap, String sourceCore, String destinationCore) {
    //Check if the core exists on both source and destination collections
    if (!sourceCoreToNodeMap.containsKey(sourceCore)) {
      logger.info("Dest core: " + destinationCore + " SrcCore: " + sourceCore + " missing in source");

      //Ok we do not have the corresponding core in the destination collection. It could indicate replica asymmetry
      logger.info("Trying to check for replica asymmetry");

      //Fetch the shard key
      String[] splits = sourceCore.split("_replica");
      String shardKey = splits[0];
      logger.info("Using " + shardKey + " as the shard key for checking other replicas..");

      //If there is a replica for the shard, it should be of the format _replica1
      String firstReplicaforCurrentShard = shardKey + "_replica1";

      if (!sourceCoreToNodeMap.containsKey(firstReplicaforCurrentShard)) {
        //No replicas exist. Skip the mapping
        logger.info("No Replica1 exist for this shard... Trying on Replica2 " + firstReplicaforCurrentShard);
        String secondReplicaforCurrentShard = shardKey + "_replica2";
        if (!sourceCoreToNodeMap.containsKey(secondReplicaforCurrentShard)) {
          logger.info("No Replica exists for this shard... Skipping " + secondReplicaforCurrentShard);
        } else {
          logger.info("Success finding an Asymetric Replica...");
          logger.info(destinationCore + " is not found on the Source Cluster  but I managed to find another Replica and I am going to use it. Found Replica " + secondReplicaforCurrentShard);
          sourceCore = secondReplicaforCurrentShard;
        }
      } else {
        //Ok we found a case where destination cluster has 2 replicas but source has only 1 replica. Map all replicas of destination to this only replica of source
        logger.info("Success finding an Asymetric Replica...");
        logger.info(destinationCore + " is not found on the Source Cluster  but I managed to find another Replica and I am going to use it. Found Replica " + firstReplicaforCurrentShard);
        sourceCore = firstReplicaforCurrentShard;
      }
    } else {
      //No Replica Asymmetry. Returning the source Core itself
      logger.info("Found " + sourceCore + "in the source Map itself...");
    }
    return sourceCore;

  }

  /**
   * Returns a mapping from core to core , where for a given shard we pick the replica with the best healthy core.
   * <p/>
   * Example: If there are 2 cores, <collection>_shard1_replica1 and <collection>_shard1_replica1 but replica2 is down and
   * replica1 is up, we map all the available cores to replica1. This guarantees high availability (even if there is 1 live
   * replica, we can recover data)
   *
   * @return {@link #getZkClusterData()} with core to best core map.
   * @throws IOException
   */
  public ZkClusterData coreToBestCoreMapping(Collection<String> collectionNames) throws IOException {
    Map<String, Map<String, String>> sourceCollectionToNodeMap = getZkClusterData().getCollectionToNodeMapping();
    Map<String, SolrCore> coreToBestReplicaMapping = new HashMap<String, SolrCore>();
    coreMapperForCluster = new SolrCoreMapper(zkClusterData, collectionNames);

    for (String sourceCollection : collectionNames) {
      Map<String, String> sourceCoreToNodeMap = sourceCollectionToNodeMap.get(sourceCollection);
      for (String srcCore : sourceCoreToNodeMap.keySet()) {
        SolrCore bestCore = coreMapperForCluster.fetchBestCoreByHealthForReplica(srcCore);
        coreToBestReplicaMapping.put(srcCore, bestCore);
      }
    }

    getZkClusterData().setCoreToBestReplicaMapping(coreToBestReplicaMapping);
    return getZkClusterData();
  }

  /**
   * Given a list of collections, retrieve its corresponding health status
   *
   * @return {@link #getZkClusterData()} with core to best core map.
   */
  public ZkClusterData fetchClusterHealth(Collection<String> collections) throws Exception {
    Map<String, Map<String, String>> sourceCollectionToNodeMap = getZkClusterData().getCollectionToNodeMapping();
    Set<SolrCore> cluterCoresStatus = new HashSet<SolrCore>();

    try {
      zookeeperHandle = ZKConnectionManager.connectToZookeeper(this.zkHost);
      setZookeeperHandle(zookeeperHandle);
      for (String sourceCollection : collections) {
        fetchCoreHealthStatus(sourceCollectionToNodeMap, cluterCoresStatus, sourceCollection);
      }
    } finally {
      getZookeeperHandle().close();
    }
    getZkClusterData().setCluterCoresStatus(cluterCoresStatus);

    return getZkClusterData();
  }

  /**
   * Given a collection to Core mapping, a collection it fetches all the solr core status
   *
   * @param sourceCollectionToNodeMap
   * @param cluterCoresStatus
   * @param sourceCollection
   */
  private void fetchCoreHealthStatus(Map<String, Map<String, String>> sourceCollectionToNodeMap, Set<SolrCore> cluterCoresStatus, String sourceCollection) {
    Map<String, String> sourceCoreToNodeMap = sourceCollectionToNodeMap.get(sourceCollection);
    for (String core : sourceCoreToNodeMap.keySet()) {
      String sourceNode = sourceCoreToNodeMap.get(core);
      SolrCore sourceSolrCore = new SolrCore(sourceNode, core);
      try {
        sourceSolrCore.loadStatus();
      } catch (Exception e) {
        logger.info("Core " + sourceSolrCore.name + " is unhealthy/unavailable...");
        sourceSolrCore.available = false;
      } finally {
        cluterCoresStatus.add(sourceSolrCore);
      }
    }
  }

  /**
   * Fetch the public DNS names for the corresponding private Ips. SolrCloud defaults to private ips for all
   * interactions. If you want to run HAFT locally to copy data across 2 different zookeeper clusters, then we need
   * public IP translations to access the index. This method helps achieve that.
   *
   * @return {@link #getZkClusterData()} with private Ip to Public DNS Mapping based on EC2 api.
   */
  public ZkClusterData translatePrivateIpToPublicHostNames() {
    AWSCredentials credentials = new BasicAWSCredentials(AwsConfigReader.fetchAccessKey(), AwsConfigReader.fetchSecretyKey());
    AmazonEC2 ec2 = new AmazonEC2Client(credentials);

    Set<String> publicDnsNameHosts = new HashSet<String>();
    Map<String, String> privateIptoPublicHostNames = new HashMap<String, String>();

    if (allSolrNodes.isEmpty()) {
      logger.info("No valid solr hosts are found. Cannot do any mapping");
      return zkClusterData;
    }


    //Describe Filter with private-ips matching all solr nodes
    DescribeInstancesRequest request = new DescribeInstancesRequest()
            .withFilters(new Filter("private-ip-address").withValues(allSolrNodes));
    DescribeInstancesResult describeInstancesResult = ec2.describeInstances(request);
    List<Reservation> reservations = describeInstancesResult.getReservations();
    //Iterate over all instances and map their private Ip to Public Host Name
    logger.info("Fetching Public HostNames....");

    for (Reservation reservation : reservations) {
      List<Instance> instances = reservation.getInstances();
      for (Instance instance : instances) {
        logger.info("Private to Public Name of the Host is " + instance.getPrivateIpAddress() + " => " + instance.getPublicDnsName());
        publicDnsNameHosts.add(instance.getPublicDnsName());
        privateIptoPublicHostNames.put(instance.getPrivateIpAddress(), instance.getPublicDnsName());
      }

    }
    //Point all zk data to point to the public dns names
    zkClusterData.updateSolrNodes(publicDnsNameHosts);
    //Set the data in a map so that it doesn't need to get recomputed by every function needing hostnames
    zkClusterData.setPrivateIpToPublicHostNameMap(privateIptoPublicHostNames);
    return zkClusterData;

  }

}


