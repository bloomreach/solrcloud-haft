package com.bloomreach.bstore.highavailability.clients;

import cascading.util.Pair;
import com.bloomreach.bstore.highavailability.actions.SourceDestCollectionMapper;
import com.bloomreach.bstore.highavailability.models.CorePairMetadata;
import com.bloomreach.bstore.highavailability.models.ZkBridgeData;
import com.bloomreach.bstore.highavailability.zookeeper.ZkClient;
import com.bloomreach.bstore.highavailability.zookeeper.ZkClusterData;
import junit.framework.TestCase;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * ZkClient Unit Testing. Setup mocks for
 * all zookeeper connections and data. Has no external dependencies
 * @author nitin,li
 * @since 6/27/14.
 */
public class ZkClientTest extends TestCase {
  //Zookeeper Connection needs to be mocked
  ZooKeeper zookeeperHandle = null;
  //ZK Data needs to be spied on since its just a POJO
  ZkClusterData data = null;
  ZkClusterData zkClusterData = null;
  //ZkClient can be mocked on to avoid connections to zk instances
  ZkClient client = null;

  @Before
  public void setUp() throws Exception {
   //Zookeeper Connection needs to be mocked
    zookeeperHandle = mock(ZooKeeper.class);

    //ZK Data needs to be spied on since its just a POJO
    data = new ZkClusterData();
    zkClusterData = spy(data);

    //ZkClient can be mocked on to avoid connections to zk instances
     client = mock(ZkClient.class);

    //Setup Zookeeper
    String clusterStateContent = new String("{\n" +
            "  \"licollection_v2\":{\n" +
            "    \"shards\":{\"shard1\":{\n" +
            "        \"range\":\"80000000-7fffffff\",\n" +
            "        \"state\":\"active\",\n" +
            "        \"replicas\":{\n" +
            "          \"core_node1\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://testhost:8983/solr\",\n" +
            "            \"core\":\"licollection_v2_shard1_replica1\",\n" +
            "            \"node_name\":\"testhost:8983_solr\",\n" +
            "            \"leader\":\"false\"},\n" +
            "          \"core_node2\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://testhost:8983/solr\",\n" +
            "            \"core\":\"licollection_v2_shard1_replica2\",\n" +
            "            \"node_name\":\"testhost:8983_solr\"}}}},\n" +
            "    \"maxShardsPerNode\":\"100\",\n" +
            "    \"router\":{\"name\":\"compositeId\"},\n" +
            "    \"replicationFactor\":\"2\"},\n" +
            "  \"nitincollection_v2\":{\n" +
            "    \"shards\":{\"shard1\":{\n" +
            "        \"range\":\"80000000-7fffffff\",\n" +
            "        \"state\":\"active\",\n" +
            "        \"replicas\":{\n" +
            "          \"core_node1\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://testhost:8983/solr\",\n" +
            "            \"core\":\"nitincollection_v2_shard1_replica2\",\n" +
            "            \"node_name\":\"testhost:8983_solr\"},\n" +
            "          \"core_node2\":{\n" +
            "            \"state\":\"down\",\n" +
            "            \"base_url\":\"http://testhost:8983/solr\",\n" +
            "            \"core\":\"nitincollection_v2_shard1_replica1\",\n" +
            "            \"node_name\":\"testhost:8983_solr\",\n" +
            "            \"leader\":\"true\"}}}},\n" +
            "    \"maxShardsPerNode\":\"100\",\n" +
            "    \"router\":{\"name\":\"compositeId\"},\n" +
            "    \"replicationFactor\":\"2\"}}");

    //Mock Zk connection to return a custom aliases when asked for aliases
    when(zookeeperHandle.getData(eq("/clusterstate.json"), (Watcher) anyObject(), (Stat) anyObject())).thenReturn(clusterStateContent.getBytes());
    List<String> nodes = new ArrayList<String>();
    nodes.add("testhost:8983_solr");
    when(zookeeperHandle.getChildren(eq("/live_nodes"), (Watcher) anyObject(), (Stat) anyObject())).thenReturn(nodes);
    List<String> configs = new ArrayList<String>();
    configs.add("collection1");
    configs.add("test_config");
    when(zookeeperHandle.getChildren(eq("/configs"), (Watcher) anyObject(), (Stat) anyObject())).thenReturn(configs);
    //Mock the client to give a mock connection and data.
    when(client.getZookeeperHandle()).thenReturn(zookeeperHandle);
    when(client.getZkClusterData()).thenReturn(zkClusterData);
    //Call the real method you want to test
    when(client.fetchZookeeperClusterState()).thenCallRealMethod();
  }

  @Test
  public void testFetchAliases() throws Exception {

    String aliasesContent = new String("{\"collection\":{\n" +
            "    \"temp_collection\":\"temp_collection_v2\",\n" +
            "    \"bloom_collection\":\"bloom_collection_v2\",\n" +
            "    \"nitin_collection\":\"nitin_collection_v2\"}}");

    //Mock Zk connection to return a custom aliases when asked for aliases
    when(zookeeperHandle.getData(eq("/aliases.json"), (Watcher) anyObject(), (Stat) anyObject())).thenReturn(aliasesContent.getBytes());

    //Mock the client to give a mock connection and data.
    when(client.getZookeeperHandle()).thenReturn(zookeeperHandle);
    when(client.getZkClusterData()).thenReturn(zkClusterData);
    //Call the real method you want to test
    when(client.fetchAlias()).thenCallRealMethod();
    //Call actual Method
    client.fetchAlias();

    //Verify all you want to verify
    assertEquals(client.getZkClusterData().getAliases().size(),3);
    assertEquals(client.getZkClusterData().getAliases().get("temp_collection"),"temp_collection_v2");
    assertEquals(client.getZkClusterData().getAliases().get("bloom_collection"),"bloom_collection_v2");
    assertEquals(client.getZkClusterData().getAliases().get("collection1"), null);

  }

  @Test
  public void testFetchClusterState() throws Exception {

    //Call actual Method
    client.fetchZookeeperClusterState();

    //Verify Replica/Shard specific Propertiess
    assertEquals(client.getZkClusterData().getClusterState().size(),2);
    assertEquals(client.getZkClusterData().getClusterState().get("nitincollection_v2").getShards().get("shard1").getRange() ,"80000000-7fffffff");
    assertEquals(client.getZkClusterData().getClusterState().get("nitincollection_v2").getShards().get("shard1").getState() ,"active");
    assertEquals(client.getZkClusterData().getClusterState().get("nitincollection_v2").getShards().get("shard1").getReplicas().get("core_node2").getLeader() ,"true");
    assertEquals(client.getZkClusterData().getClusterState().get("nitincollection_v2").getShards().get("shard1").getReplicas().get("core_node1").getLeader() , null);
    assertEquals(client.getZkClusterData().getClusterState().get("nitincollection_v2").getShards().get("shard1").getReplicas().get("core_node2").getState() ,"down");


    //Verify Range keys, routers and cluster keys
    assertEquals(client.getZkClusterData().getClusterState().get("licollection_v2").getShards().get("shard1").getRange() ,"80000000-7fffffff");
    assertEquals(client.getZkClusterData().getClusterState().get("licollection_v2").getShards().get("shard1").getState() ,"active");
    assertEquals(client.getZkClusterData().getClusterState().get("licollection_v2").getShards().get("shard1").getReplicas().get("core_node1").getLeader() ,"false");
    assertEquals(client.getZkClusterData().getClusterState().get("nitincollection_v2").getMaxShardsPerNode(),"100");
    assertEquals(client.getZkClusterData().getClusterState().get("nitincollection_v2").getReplicationFactor(),"2");

  }

  @Test
  public void testFetchSolrNodes() throws Exception {

    when(client.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client.fetchLiveSolrNodes()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all solr nodes
    client.fetchLiveSolrNodes();

    assertEquals(client.getZkClusterData().getClusterState().get("licollection_v2").getShards().get("shard1").getRange() ,"80000000-7fffffff");
    assertTrue(client.getZkClusterData().getSolrHosts().contains("testhost"));
  }

  @Test
  public void testFetchCollectionToNodeMapping () throws Exception {

    when(client.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client.fetchCollectionToNodeMapping()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all solr nodes
    client.fetchCollectionToNodeMapping();

    assertEquals(client.getZkClusterData().getClusterState().get("licollection_v2").getShards().get("shard1").getRange() ,"80000000-7fffffff");
    Map<String, Map<String, String>> collectionToNodeMapping =  client.getZkClusterData().getCollectionToNodeMapping();
    assertEquals(collectionToNodeMapping.get("licollection_v2").get("licollection_v2_shard1_replica2"), "testhost" );
    assertEquals(collectionToNodeMapping.get("licollection_v2").get("licollection_v2_shard1_replica1"), "testhost" );
    assertEquals(collectionToNodeMapping.get("nitincollection_v2").get("nitincollection_v2_shard1_replica2"), "testhost" );
    assertEquals(collectionToNodeMapping.get("nitincollection_v2").get("nitincollection_v2_shard1_replica1"), "testhost" );
  }

  @Test
  public void testFetchAllCollections () throws Exception {
    //Call real method for solrNodes
    when(client.fetchAllCollections()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all solr nodes
    client.fetchAllCollections();

    assertEquals(client.getZkClusterData().getClusterState().get("licollection_v2").getShards().get("shard1").getRange() ,"80000000-7fffffff");
    Collection<String> collections =  client.getZkClusterData().getCollections();
    assertTrue(collections.contains("licollection_v2"));
    assertTrue(collections.contains("nitincollection_v2"));

  }

  @Test
  public void testFetchNodeToCoreMappings() throws Exception {

    when(client.fetchNodeName(anyString())).thenCallRealMethod();

    //Call real method for solrNodes
    when(client.fetchNodeToCoreMapping()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all solr nodes
    client.fetchNodeToCoreMapping();

    assertEquals(client.getZkClusterData().getClusterState().get("licollection_v2").getShards().get("shard1").getRange() ,"80000000-7fffffff");
    Map<String, List<String>> nodeToCoreMap =  client.getZkClusterData().getNodeToCoreMapping();
    List<String> allCollections =  nodeToCoreMap.get("testhost");
    assertTrue(allCollections.contains("nitincollection_v2_shard1_replica1"));
    assertTrue(allCollections.contains("nitincollection_v2_shard1_replica2"));
    assertTrue(allCollections.contains("licollection_v2_shard1_replica1"));
    assertTrue(allCollections.contains("licollection_v2_shard1_replica2"));

  }

  @Test
  public void testFetchConfigs() throws Exception {

    when(client.fetchNodeName(anyString())).thenCallRealMethod();

    //Call real method for solrNodes
    when(client.fetchAllConfigs()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all solr nodes
    client.fetchAllConfigs();

    assertEquals(client.getZkClusterData().getClusterState().get("licollection_v2").getShards().get("shard1").getRange() ,"80000000-7fffffff");
    Collection<String> allConfigurations =  client.getZkClusterData().getConfigs();
    assertTrue(allConfigurations.contains("collection1"));
    assertTrue(allConfigurations.contains("test_config"));
    assertFalse(allConfigurations.contains("bloomreach_config"));

  }


  @Test
  public void testAsymmetricCollectionPeers() throws Exception {

    when(client.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client.fetchCollectionToNodeMapping()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all solr nodes
    client.fetchCollectionToNodeMapping();


    //Setup Second Zookeeper Stats

    //Zookeeper Connection needs to be mocked
    ZooKeeper newZkHandle = mock(ZooKeeper.class);

    //ZK Data needs to be spied on since its just a POJO
    ZkClusterData data1 = new ZkClusterData();
    ZkClusterData dataSpy = spy(data1);

    //ZkClient can be mocked on to avoid connections to zk instances
    ZkClient client1 = mock(ZkClient.class);

    //Setup Zookeeper
    String clusterStateContent = new String("{\n" +
            "  \"nitincollection_v2\":{\n" +
            "    \"shards\":{\"shard1\":{\n" +
            "        \"range\":\"80000000-7fffffff\",\n" +
            "        \"state\":\"active\",\n" +
            "        \"replicas\":{\n" +
            "          \"core_node1\":{\n" +
            "            \"state\":\"down\",\n" +
            "            \"base_url\":\"http://oneshardhost:8983/solr\",\n" +
            "            \"core\":\"nitincollection_v2_shard1_replica1\",\n" +
            "            \"node_name\":\"oneshardhost:8983_solr\",\n" +
            "            \"leader\":\"true\"}}}},\n" +
            "    \"maxShardsPerNode\":\"100\",\n" +
            "    \"router\":{\"name\":\"compositeId\"},\n" +
            "    \"replicationFactor\":\"2\"}}");

    //Mock Zk connection to return a custom aliases when asked for aliases
    when(newZkHandle.getData(eq("/clusterstate.json"), (Watcher) anyObject(), (Stat) anyObject())).thenReturn(clusterStateContent.getBytes());
    //Mock the client to give a mock connection and data.
    when(client1.getZookeeperHandle()).thenReturn(newZkHandle);
    when(client1.getZkClusterData()).thenReturn(dataSpy);
    //Call the real method you want to test
    when(client1.fetchZookeeperClusterState()).thenCallRealMethod();



    when(client1.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client1.fetchCollectionToNodeMapping()).thenCallRealMethod();

    client1.fetchZookeeperClusterState();
    client1.fetchCollectionToNodeMapping();

    ZkClusterData twoShardedZkClusterData = client.getZkClusterData();
    SourceDestCollectionMapper collectionMapper = new SourceDestCollectionMapper("_collection:_collection,default:_collection", "collection:test_config,default:test_config", new ZkClusterData() );
    Collection<String> collections = new ArrayList<String>();
    collections.add("nitincollection_v2");

    //Call real method for solrNodes
    when(client1.matchSourceToDestinationSolrCollectionPeers(eq(twoShardedZkClusterData), eq(collectionMapper) , eq(collections))).thenCallRealMethod();

    ZkBridgeData bridgeData = client1.matchSourceToDestinationSolrCollectionPeers(twoShardedZkClusterData, collectionMapper, collections);
    Map<String, Pair<CorePairMetadata,CorePairMetadata>> coreToPeerMap =  bridgeData.getSourceToDestionationPeerMapping().get("nitincollection_v2");
    //Assert that the first replica is mapped correctly
    assertEquals(coreToPeerMap.get("nitincollection_v2_shard1_replica1").getLhs().getHostName(), ("testhost"));
    assertEquals(coreToPeerMap.get("nitincollection_v2_shard1_replica1").getRhs().getHostName(), ("oneshardhost"));

    //Meat of the checks. Even though there is no replica2 on the source cluster, the destination cluster's replica2 should be mapped to source's replica1
    assertEquals(coreToPeerMap.get("nitincollection_v2_shard1_replica2").getLhs().getHostName(), ("testhost"));
    assertEquals(coreToPeerMap.get("nitincollection_v2_shard1_replica2").getRhs().getHostName(), ("oneshardhost"));

    //Assert that the other collections (not interested ones) are not populated
    assertNull(coreToPeerMap.get("licollection_v2_shard1_replica1"));
    assertNull(coreToPeerMap.get("licollection_v2_shard1_replica2"));

  }



  @Test
  public void testSymmetricCollectionPeers() throws Exception {

    when(client.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client.fetchCollectionToNodeMapping()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all solr nodes
    client.fetchCollectionToNodeMapping();


    //Setup Second Zookeeper Stats

    //Zookeeper Connection needs to be mocked
    ZooKeeper newZkHandle = mock(ZooKeeper.class);

    //ZK Data needs to be spied on since its just a POJO
    ZkClusterData data1 = new ZkClusterData();
    ZkClusterData dataSpy = spy(data1);

    //ZkClient can be mocked on to avoid connections to zk instances
    ZkClient client1 = mock(ZkClient.class);

    //Setup Zookeeper
    String clusterStateContent = new String("{\n" +
            "  \"nitincollection_v2\":{\n" +
            "    \"shards\":{\"shard1\":{\n" +
            "        \"range\":\"80000000-7fffffff\",\n" +
            "        \"state\":\"active\",\n" +
            "        \"replicas\":{\n" +
            "          \"core_node1\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://shardonereplicatwohost:8983/solr\",\n" +
            "            \"core\":\"nitincollection_v2_shard1_replica2\",\n" +
            "            \"node_name\":\"shardonereplicatwohost:8983_solr\"},\n" +
            "          \"core_node2\":{\n" +
            "            \"state\":\"down\",\n" +
            "            \"base_url\":\"http://shardonereplicaonehost:8983/solr\",\n" +
            "            \"core\":\"nitincollection_v2_shard1_replica1\",\n" +
            "            \"node_name\":\"shardonereplicaonehost:8983_solr\",\n" +
            "            \"leader\":\"true\"}}}},\n" +
            "    \"maxShardsPerNode\":\"100\",\n" +
            "    \"router\":{\"name\":\"compositeId\"},\n" +
            "    \"replicationFactor\":\"2\"}}");

    //Mock Zk connection to return a custom aliases when asked for aliases
    when(newZkHandle.getData(eq("/clusterstate.json"), (Watcher) anyObject(), (Stat) anyObject())).thenReturn(clusterStateContent.getBytes());
    //Mock the client to give a mock connection and data.
    when(client1.getZookeeperHandle()).thenReturn(newZkHandle);
    when(client1.getZkClusterData()).thenReturn(dataSpy);
    //Call the real method you want to test
    when(client1.fetchZookeeperClusterState()).thenCallRealMethod();



    when(client1.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client1.fetchCollectionToNodeMapping()).thenCallRealMethod();

    client1.fetchZookeeperClusterState();
    client1.fetchCollectionToNodeMapping();

    ZkClusterData twoShardedZkClusterData = client.getZkClusterData();
    SourceDestCollectionMapper collectionMapper = new SourceDestCollectionMapper("_collection:_collection,default:_collection", "collection:test_config,default:test_config", new ZkClusterData() );
    Collection<String> collections = new ArrayList<String>();
    collections.add("nitincollection_v2");

    //Call real method for solrNodes
    when(client1.matchSourceToDestinationSolrCollectionPeers(eq(twoShardedZkClusterData), eq(collectionMapper) , eq(collections))).thenCallRealMethod();

    ZkBridgeData bridgeData = client1.matchSourceToDestinationSolrCollectionPeers(twoShardedZkClusterData, collectionMapper, collections);
    Map<String, Pair<CorePairMetadata,CorePairMetadata>> coreToPeerMap =  bridgeData.getSourceToDestionationPeerMapping().get("nitincollection_v2");
    //Assert that the first replica is mapped correctly
    assertEquals(coreToPeerMap.get("nitincollection_v2_shard1_replica1").getLhs().getHostName(), ("testhost"));
    assertEquals(coreToPeerMap.get("nitincollection_v2_shard1_replica1").getRhs().getHostName(), ("shardonereplicaonehost"));

    //Meat of the checks. Even though there is no replica2 on the source cluster, the destination cluster's replica2 should be mapped to source's replica1
    assertEquals(coreToPeerMap.get("nitincollection_v2_shard1_replica2").getLhs().getHostName(), ("testhost"));
    assertEquals(coreToPeerMap.get("nitincollection_v2_shard1_replica2").getRhs().getHostName(), ("shardonereplicatwohost"));

    //Assert that the other collections (not interested ones) are not populated
    assertNull(coreToPeerMap.get("licollection_v2_shard1_replica1"));
    assertNull(coreToPeerMap.get("licollection_v2_shard1_replica2"));

  }




  @Test
  public void testSymmetricReplicationMultipleCollections() throws Exception {

    when(client.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client.fetchCollectionToNodeMapping()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all solr nodes
    client.fetchCollectionToNodeMapping();


    //Setup Second Zookeeper Stats

    //Zookeeper Connection needs to be mocked
    ZooKeeper newZkHandle = mock(ZooKeeper.class);

    //ZK Data needs to be spied on since its just a POJO
    ZkClusterData data1 = new ZkClusterData();
    ZkClusterData dataSpy = spy(data1);

    //ZkClient can be mocked on to avoid connections to zk instances
    ZkClient client1 = mock(ZkClient.class);

    //Setup Zookeeper
    //Setup Zookeeper
    String clusterStateContent = new String("{\n" +
            "  \"licollection_v2\":{\n" +
            "    \"shards\":{\"shard1\":{\n" +
            "        \"range\":\"80000000-7fffffff\",\n" +
            "        \"state\":\"active\",\n" +
            "        \"replicas\":{\n" +
            "          \"core_node1\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://licollectionproductshardonereplicaonehost:8983/solr\",\n" +
            "            \"core\":\"licollection_v2_shard1_replica1\",\n" +
            "            \"node_name\":\"licollectionproductshardonereplicaonehost:8983_solr\",\n" +
            "            \"leader\":\"false\"},\n" +
            "          \"core_node2\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://licollectionproductshardonereplicatwohost:8983/solr\",\n" +
            "            \"core\":\"licollection_v2_shard1_replica2\",\n" +
            "            \"node_name\":\"licollectionproductshardonereplicatwohost:8983_solr\"}}}},\n" +
            "    \"maxShardsPerNode\":\"100\",\n" +
            "    \"router\":{\"name\":\"compositeId\"},\n" +
            "    \"replicationFactor\":\"2\"},\n" +
            "  \"nitincollection_v2\":{\n" +
            "    \"shards\":{\"shard1\":{\n" +
            "        \"range\":\"80000000-7fffffff\",\n" +
            "        \"state\":\"active\",\n" +
            "        \"replicas\":{\n" +
            "          \"core_node1\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://nitincollectionproductshardonereplicatwohost:8983/solr\",\n" +
            "            \"core\":\"nitincollection_v2_shard1_replica2\",\n" +
            "            \"node_name\":\"nitincollectionproductshardonereplicatwohost:8983_solr\"},\n" +
            "          \"core_node2\":{\n" +
            "            \"state\":\"down\",\n" +
            "            \"base_url\":\"http://nitincollectionproductshardonereplicaonehost:8983/solr\",\n" +
            "            \"core\":\"nitincollection_v2_shard1_replica1\",\n" +
            "            \"node_name\":\"nitincollectionproductshardonereplicaonehost:8983_solr\",\n" +
            "            \"leader\":\"true\"}}}},\n" +
            "    \"maxShardsPerNode\":\"100\",\n" +
            "    \"router\":{\"name\":\"compositeId\"},\n" +
            "    \"replicationFactor\":\"2\"}}");

    //Mock Zk connection to return a custom aliases when asked for aliases
    when(newZkHandle.getData(eq("/clusterstate.json"), (Watcher) anyObject(), (Stat) anyObject())).thenReturn(clusterStateContent.getBytes());
    //Mock the client to give a mock connection and data.
    when(client1.getZookeeperHandle()).thenReturn(newZkHandle);
    when(client1.getZkClusterData()).thenReturn(dataSpy);
    //Call the real method you want to test
    when(client1.fetchZookeeperClusterState()).thenCallRealMethod();



    when(client1.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client1.fetchCollectionToNodeMapping()).thenCallRealMethod();

    client1.fetchZookeeperClusterState();
    client1.fetchCollectionToNodeMapping();

    ZkClusterData twoShardedZkClusterData = client.getZkClusterData();
    SourceDestCollectionMapper collectionMapper = new SourceDestCollectionMapper("_collection:_collection,default:_collection", "collection:test_config,default:test_config", new ZkClusterData() );
    Collection<String> collections = new ArrayList<String>();
    collections.add("nitincollection_v2");
    collections.add("licollection_v2");

    //Call real method for solrNodes
    when(client1.matchSourceToDestinationSolrCollectionPeers(eq(twoShardedZkClusterData), eq(collectionMapper) , eq(collections))).thenCallRealMethod();

    ZkBridgeData bridgeData = client1.matchSourceToDestinationSolrCollectionPeers(twoShardedZkClusterData, collectionMapper, collections);
    Map<String, Pair<CorePairMetadata,CorePairMetadata>> nitinPeerMap =  bridgeData.getSourceToDestionationPeerMapping().get("nitincollection_v2");
    //Assert that the first replica is mapped correctly
    assertEquals(nitinPeerMap.get("nitincollection_v2_shard1_replica1").getLhs().getHostName(), ("testhost"));
    assertEquals(nitinPeerMap.get("nitincollection_v2_shard1_replica1").getRhs().getHostName(), ("nitincollectionproductshardonereplicaonehost"));

    //Meat of the checks. Even though there is no replica2 on the source cluster, the destination cluster's replica2 should be mapped to source's replica1
    assertEquals(nitinPeerMap.get("nitincollection_v2_shard1_replica2").getLhs().getHostName(), ("testhost"));
    assertEquals(nitinPeerMap.get("nitincollection_v2_shard1_replica2").getRhs().getHostName(), ("nitincollectionproductshardonereplicatwohost"));

    Map<String, Pair<CorePairMetadata,CorePairMetadata>> pbteenNonProductCoreToPeerMap =  bridgeData.getSourceToDestionationPeerMapping().get("licollection_v2");


    //Assert Non Product collections also
    assertEquals(pbteenNonProductCoreToPeerMap.get("licollection_v2_shard1_replica1").getLhs().getHostName(), ("testhost"));
    assertEquals(pbteenNonProductCoreToPeerMap.get("licollection_v2_shard1_replica1").getRhs().getHostName(), ("licollectionproductshardonereplicaonehost"));

    assertEquals(pbteenNonProductCoreToPeerMap.get("licollection_v2_shard1_replica2").getLhs().getHostName(), ("testhost"));
    assertEquals(pbteenNonProductCoreToPeerMap.get("licollection_v2_shard1_replica2").getRhs().getHostName(), ("licollectionproductshardonereplicatwohost"));
  }


  @Test
  public void testASymmetricReplicationMultipleCollections() throws Exception {

    when(client.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client.fetchCollectionToNodeMapping()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all solr nodes
    client.fetchCollectionToNodeMapping();


    //Setup Second Zookeeper Stats

    //Zookeeper Connection needs to be mocked
    ZooKeeper newZkHandle = mock(ZooKeeper.class);

    //ZK Data needs to be spied on since its just a POJO
    ZkClusterData data1 = new ZkClusterData();
    ZkClusterData dataSpy = spy(data1);

    //ZkClient can be mocked on to avoid connections to zk instances
    ZkClient client1 = mock(ZkClient.class);

    //Setup Zookeeper
    //Setup Zookeeper
    String clusterStateContent = new String("{\n" +
            "  \"licollection_v2\":{\n" +
            "    \"shards\":{\"shard1\":{\n" +
            "        \"range\":\"80000000-7fffffff\",\n" +
            "        \"state\":\"active\",\n" +
            "        \"replicas\":{\n" +
            "          \"core_node2\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://licollectionproductshardonereplicaonehost:8983/solr\",\n" +
            "            \"core\":\"licollection_v2_shard1_replica1\",\n" +
            "            \"node_name\":\"licollectionproductshardonereplicaonehost:8983_solr\"}}}},\n" +
            "    \"maxShardsPerNode\":\"100\",\n" +
            "    \"router\":{\"name\":\"compositeId\"},\n" +
            "    \"replicationFactor\":\"2\"},\n" +
            "  \"nitincollection_v2\":{\n" +
            "    \"shards\":{\"shard1\":{\n" +
            "        \"range\":\"80000000-7fffffff\",\n" +
            "        \"state\":\"active\",\n" +
            "        \"replicas\":{\n" +
            "          \"core_node2\":{\n" +
            "            \"state\":\"down\",\n" +
            "            \"base_url\":\"http://nitincollectionproductshardonereplicaonehost:8983/solr\",\n" +
            "            \"core\":\"nitincollection_v2_shard1_replica1\",\n" +
            "            \"node_name\":\"nitincollectionproductshardonereplicaonehost:8983_solr\",\n" +
            "            \"leader\":\"true\"}}}},\n" +
            "    \"maxShardsPerNode\":\"100\",\n" +
            "    \"router\":{\"name\":\"compositeId\"},\n" +
            "    \"replicationFactor\":\"2\"}}");

    //Mock Zk connection to return a custom aliases when asked for aliases
    when(newZkHandle.getData(eq("/clusterstate.json"), (Watcher) anyObject(), (Stat) anyObject())).thenReturn(clusterStateContent.getBytes());
    //Mock the client to give a mock connection and data.
    when(client1.getZookeeperHandle()).thenReturn(newZkHandle);
    when(client1.getZkClusterData()).thenReturn(dataSpy);
    //Call the real method you want to test
    when(client1.fetchZookeeperClusterState()).thenCallRealMethod();



    when(client1.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client1.fetchCollectionToNodeMapping()).thenCallRealMethod();

    client1.fetchZookeeperClusterState();
    client1.fetchCollectionToNodeMapping();

    ZkClusterData twoShardedZkClusterData = client.getZkClusterData();
    SourceDestCollectionMapper collectionMapper = new SourceDestCollectionMapper("_collection:_collection,default:_collection", "collection:test_config,default:test_config", new ZkClusterData() );
    Collection<String> collections = new ArrayList<String>();
    collections.add("nitincollection_v2");
    collections.add("licollection_v2");

    //Call real method for solrNodes
    when(client1.matchSourceToDestinationSolrCollectionPeers(eq(twoShardedZkClusterData), eq(collectionMapper) , eq(collections))).thenCallRealMethod();

    ZkBridgeData bridgeData = client1.matchSourceToDestinationSolrCollectionPeers(twoShardedZkClusterData, collectionMapper, collections);
    Map<String, Pair<CorePairMetadata,CorePairMetadata>> nitinCollectionPeerMap =  bridgeData.getSourceToDestionationPeerMapping().get("nitincollection_v2");
    //Assert that the first replica is mapped correctly
    assertEquals(nitinCollectionPeerMap.get("nitincollection_v2_shard1_replica1").getLhs().getHostName(), ("testhost"));
    assertEquals(nitinCollectionPeerMap.get("nitincollection_v2_shard1_replica1").getRhs().getHostName(), ("nitincollectionproductshardonereplicaonehost"));

    //Meat of the checks. Even though there is no replica2 on the source cluster, the destination cluster's replica2 should be mapped to source's replica1
    assertEquals(nitinCollectionPeerMap.get("nitincollection_v2_shard1_replica2").getLhs().getHostName(), ("testhost"));
    assertEquals(nitinCollectionPeerMap.get("nitincollection_v2_shard1_replica2").getRhs().getHostName(), ("nitincollectionproductshardonereplicaonehost"));

    Map<String, Pair<CorePairMetadata,CorePairMetadata>> liCollectionPeerMap =  bridgeData.getSourceToDestionationPeerMapping().get("licollection_v2");

    //Assert that the first replica is mapped correctly
    assertEquals(liCollectionPeerMap.get("licollection_v2_shard1_replica1").getLhs().getHostName(), ("testhost"));
    assertEquals(liCollectionPeerMap.get("licollection_v2_shard1_replica1").getRhs().getHostName(), ("licollectionproductshardonereplicaonehost"));

    //Meat of the checks. Even though there is no replica2 on the source cluster, the destination cluster's replica2 should be mapped to source's replica1
    assertEquals(liCollectionPeerMap.get("licollection_v2_shard1_replica2").getLhs().getHostName(), ("testhost"));
    assertEquals(liCollectionPeerMap.get("licollection_v2_shard1_replica2").getRhs().getHostName(), ("licollectionproductshardonereplicaonehost"));
  }

  @Test
  public void testMultipleReplicas() throws Exception {

    when(client.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client.fetchCollectionToNodeMapping()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all solr nodes
    client.fetchCollectionToNodeMapping();


    //Setup Second Zookeeper Stats

    //Zookeeper Connection needs to be mocked
    ZooKeeper newZkHandle = mock(ZooKeeper.class);

    //ZK Data needs to be spied on since its just a POJO
    ZkClusterData data1 = new ZkClusterData();
    ZkClusterData dataSpy = spy(data1);

    //ZkClient can be mocked on to avoid connections to zk instances
    ZkClient client1 = mock(ZkClient.class);

    //Setup Zookeeper
    String clusterStateContent = new String("{\n" +
            "  \"licollection_v2\":{\n" +
            "    \"shards\":{\"shard1\":{\n" +
            "        \"range\":\"80000000-7fffffff\",\n" +
            "        \"state\":\"active\",\n" +
            "        \"replicas\":{\n" +
            "          \"core_node1\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://licollectionproductshardonereplicaonehost:8983/solr\",\n" +
            "            \"core\":\"licollection_v2_shard1_replica1\",\n" +
            "            \"node_name\":\"licollectionproductshardonereplicaonehost:8983_solr\",\n" +
            "            \"leader\":\"false\"},\n" +
            "          \"core_node2\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://licollectionproductshardonereplicatwohost:8983/solr\",\n" +
            "            \"core\":\"licollection_v2_shard1_replica2\",\n" +
            "            \"node_name\":\"licollectionproductshardonereplicatwohost:8983_solr\"},\n" +
            "          \"core_node3\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://licollectionproductshardonereplicathreehost:8983/solr\",\n" +
            "            \"core\":\"licollection_v2_shard1_replica3\",\n" +
            "            \"node_name\":\"licollectionproductshardonereplicathreehost:8983_solr\"},\n" +
            "          \"core_node4\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://licollectionproductshardonereplicafourhost:8983/solr\",\n" +
            "            \"core\":\"licollection_v2_shard1_replica4\",\n" +
            "            \"node_name\":\"licollectionproductshardonereplicafourhost:8983_solr\"},\n" +
            "          \"core_node5\":{\n" +
            "            \"state\":\"active\",\n" +
            "            \"base_url\":\"http://licollectionproductshardonereplicafivehost:8983/solr\",\n" +
            "            \"core\":\"licollection_v2_shard1_replica5\",\n" +
            "            \"node_name\":\"licollectionproductshardonereplicafivehost:8983_solr\"}}}},\n" +
            "    \"maxShardsPerNode\":\"100\",\n" +
            "    \"router\":{\"name\":\"compositeId\"},\n" +
            "    \"replicationFactor\":\"4\"}}");

    //Mock Zk connection to return a custom aliases when asked for aliases
    when(newZkHandle.getData(eq("/clusterstate.json"), (Watcher) anyObject(), (Stat) anyObject())).thenReturn(clusterStateContent.getBytes());
    //Mock the client to give a mock connection and data.
    when(client1.getZookeeperHandle()).thenReturn(newZkHandle);
    when(client1.getZkClusterData()).thenReturn(dataSpy);
    //Call the real method you want to test
    when(client1.fetchZookeeperClusterState()).thenCallRealMethod();



    when(client1.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client1.fetchCollectionToNodeMapping()).thenCallRealMethod();

    client1.fetchZookeeperClusterState();
    client1.fetchCollectionToNodeMapping();



    ZkClusterData twoShardedZkClusterData = client1.getZkClusterData();
    for (String  uberMapKey: twoShardedZkClusterData.getCollectionToNodeMapping().keySet()) {
      Map<String,String> insideMap = twoShardedZkClusterData.getCollectionToNodeMapping().get(uberMapKey);
      for (String key: insideMap.keySet()) {
        System.out.println(key + ": " + insideMap.get(key));
      }

    }
    SourceDestCollectionMapper collectionMapper = new SourceDestCollectionMapper("_collection_v2:_collection_v2,default:_collection", "collection_v2:test_config,default:test_config", new ZkClusterData() );
    Collection<String> collections = new ArrayList<String>();
    collections.add("licollection_v2");

    //Call real method for solrNodes
    when(client.matchSourceToDestinationSolrCollectionPeers(eq(twoShardedZkClusterData), eq(collectionMapper) , eq(collections))).thenCallRealMethod();

    ZkBridgeData bridgeData = client.matchSourceToDestinationSolrCollectionPeers(twoShardedZkClusterData, collectionMapper, collections);
    Map<String, Pair<CorePairMetadata,CorePairMetadata>> coreToPeerMap =  bridgeData.getSourceToDestionationPeerMapping().get("licollection_v2");
    //Assert that the first replica is mapped correctly
    assertEquals(coreToPeerMap.get("licollection_v2_shard1_replica1").getRhs().getHostName(), ("testhost"));
    assertEquals(coreToPeerMap.get("licollection_v2_shard1_replica1").getLhs().getHostName(), ("licollectionproductshardonereplicaonehost"));

    //Assert that the first replica is mapped correctly
    assertEquals(coreToPeerMap.get("licollection_v2_shard1_replica2").getRhs().getHostName(), ("testhost"));
    assertEquals(coreToPeerMap.get("licollection_v2_shard1_replica2").getLhs().getHostName(), ("licollectionproductshardonereplicatwohost"));

    assertEquals(coreToPeerMap.get("licollection_v2_shard1_replica3").getRhs().getHostName(), ("testhost"));
    assertEquals(coreToPeerMap.get("licollection_v2_shard1_replica3").getLhs().getHostName(), ("licollectionproductshardonereplicathreehost"));

    assertEquals(coreToPeerMap.get("licollection_v2_shard1_replica4").getRhs().getHostName(), ("testhost"));
    assertEquals(coreToPeerMap.get("licollection_v2_shard1_replica4").getLhs().getHostName(), ("licollectionproductshardonereplicafourhost"));

    //Assert That the other replica is left alone..
    assertNull(coreToPeerMap.get("nitincollection_shard1_replica2"));
  }


  @Test
  public void testFetchLeaderStatus() throws Exception {

    when(client.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client.fetchLeaderStatus()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all solr nodes
    client.fetchLeaderStatus();

    assertEquals(client.getZkClusterData().getClusterState().get("licollection_v2").getShards().get("shard1").getRange() ,"80000000-7fffffff");
    Map<String,Map<String,String>> leaderStatus = client.getZkClusterData().getCollectionToShardLeaderMapping();
    assertEquals(leaderStatus.get("nitincollection_v2").get("shard1"), "testhost:8983_solr,nitincollection_v2_shard1_replica1");
    assertEquals(leaderStatus.get("licollection_v2").get("shard1"), ZkClient.EMPTY_LEADER);

  }

  @Test
  public void testCollectionToShardToCoreMapping() throws Exception {
    when(client.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client.fetchCollectionToShardToCoreMapping()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all Collection TO Shard To Core Mapping
    client.fetchCollectionToShardToCoreMapping();

    assertEquals(client.getZkClusterData().getClusterState().get("licollection_v2").getShards().get("shard1").getRange() ,"80000000-7fffffff");
    Map<String, Map<String, Map<String, String>>> collectionToNodeMapping =  client.getZkClusterData().getCollectionToShardToCoreMapping();
    assertEquals(collectionToNodeMapping.get("licollection_v2").get("shard1").get("licollection_v2_shard1_replica2"), "testhost" );
    assertEquals(collectionToNodeMapping.get("licollection_v2").get("shard1").get("licollection_v2_shard1_replica1"), "testhost" );
    assertEquals(collectionToNodeMapping.get("nitincollection_v2").get("shard1").get("nitincollection_v2_shard1_replica2"), "testhost" );
    assertEquals(collectionToNodeMapping.get("nitincollection_v2").get("shard1").get("nitincollection_v2_shard1_replica1"), "testhost");
  }


  @Test
  public void testCollectionWithCustomMapping() throws Exception {
    when(client.fetchNodeName(anyString())).thenCallRealMethod();
    //Call real method for solrNodes
    when(client.fetchCollectionToShardToCoreMapping()).thenCallRealMethod();

    //Call actual Method
    client.fetchZookeeperClusterState();
    //Fetch all Collection TO Shard To Core Mapping
    client.fetchCollectionToShardToCoreMapping();

    assertEquals(client.getZkClusterData().getClusterState().get("licollection_v2").getShards().get("shard1").getRange() ,"80000000-7fffffff");
    Map<String, Map<String, Map<String, String>>> collectionToNodeMapping =  client.getZkClusterData().getCollectionToShardToCoreMapping();
    assertEquals(collectionToNodeMapping.get("nitincollection_v2").get("shard1").get("nitincollection_v2_shard1_replica2"), "testhost" );
    assertEquals(collectionToNodeMapping.get("nitincollection_v2").get("shard1").get("nitincollection_v2_shard1_replica1"), "testhost");
  }


  @After
  public void tearDown() throws Exception {
    zookeeperHandle = null;
    data = null;
    zkClusterData = null;
    client = null;
  }


}
