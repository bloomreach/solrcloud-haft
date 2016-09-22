# Introduction

HAFT is a High Availability and Fault Tolerant Framework for [SolrCloud]. The aim of the library
is to provide HA and reliability features for SolrCloud. Everything in HAFT is organized as a FaultTolerant (FT) Action. An FT Action is anything
that interacts with Zookeeper or SolrCloud and providing a high availability feature.

A few examples of FT Actions are


  - Cloning SolrCloud Clusters & Collections
  - Backup SolrCloud Index
  - Backing up Zookeeper data
  - Adding a Replica
  - Replacing a Dead Node
  - Adding nodes on the fly

For the initial release, we are open sourcing SolrCloud Collection Cloning, Backup and Zk Config Replication Actions.
HAFT has been tested with SolrCloud 4.6.1. We are still working on SolrCloud 5.0 Compatibility


# Building with Tests using Maven
```sh
 cd solrcloud-haft/
 mvn clean && mvn install -Pshade
```

# Running an existing Cluster Backup Example
  - This assumes [zk ensemble] runs at 2181 and solrcloud runs at 8983.

  - HAFT uses private ips by default. If you are running haft inside your data center, it will work out of the box.

  - If you are running it in your local machine, you need to put your aws credentials under resources/aws_credentials.properties. HAFT will
    use that to map private ip to public host names using EC2 Api. We do not support google app engine just yet.

  - This example takes a source zk (from an ensemble) and destination zk host (from another ensemble) as parameter
    and clones all the collections from source to destination solrcloud cluster.


```sh
#Example 1:  Clone all collections from source to destination (with the same collection names as the source).

  - cd solrcloud-haft/target/
  - java -cp uber-solrcloud-haft-final.jar com.bloomreach.bstore.highavailability.clients.SolrCloudCrossClusterBackup \
    <sourck-zk>:2181 <destinationzk>:2181
```


```sh
#Example 2:  Clone all collections from source to destination (created with suffix "_collectionnew").

  - cd solrcloud-haft/target/
  - java -cp uber-solrcloud-haft-final.jar com.bloomreach.bstore.highavailability.clients.SolrCloudCrossClusterBackupWithNewCollectionNames \
   <sourck-zk>:2181 <destinationzk>:2181
```


# Building Blocks of HAFT

  - <b>FaultTolerant Action</b> A fault Tolerant Action that interacts with zookeeper/solrcloud cluster to perform a high available action.
  - <b>Operation Config:</b> A Config Wrapper that helps control the knobs of all FT Actions.
  - <b>ZKClient:</b> Provides an in memory data representation of the solrcloud cluster and collection state.


# Cloning Zookeeper Configs
 - Given a source zookeeper and destination zookeeper, clones all the [configs] from the source to destination zookeeper
 - Define an Operation Config all the knobs required and "clone_zk" as the operation.
 - Fetch the action from the Factory and execute it.

```sh
 //Define the Operation Config
 OperationConfig config = new OperationConfig();

 //Set source Zookeeper Host to clone from
 config.setZkHost(sourceZk);

 //Set the zk root to copy configs
 config.setConfigRoot("/configs");

 //Set source Zookeeper Host to clone to
 config.setDestinationZkHost(destinationZookeeper);

 //Set the action to be performed
 config.setAction(FTActions.CLONE_ZK.getAction());

 //Fetch the action instance from the Factory based on the operation Config.
 SolrFaultTolerantAction zkConfigsAction = SolrActionFactory.fetchActionToPerform(config);

 //Execute the action
 zkConfigsAction.executeAction();
```

# Cloning SolrCloud Collections
 - Given a source zookeeper and destination zookeeper, clones all the [collections] from the source to destination zookeeper
 - Define an Operation Config all the knobs required and "clone" as the operation.
 - Fetch the action from the Factory and execute it.

```sh
  //Create a new Operation Config
  OperationConfig cloneCollectionConfig = new OperationConfig();

  //Set source Zookeeper Host to clone from
  cloneCollectionConfig.setZkHost(sourceZk);

  //Set source Zookeeper Host to clone into
  cloneCollectionConfig.setDestinationZkHost(destinationZookeeper);

  //Set solrcollections to clone all collections.
  List<String> solrCollections = new ArrayList<String>();
  solrCollections.add("all");
  cloneCollectionConfig.setCollections(solrCollections);

  //Set the desired number of replicas in the destination cluster
  cloneCollectionConfig.setReplicationFactor("1");

  //Set the action to be performed
  cloneCollectionConfig.setAction(FTActions.CLONE.getAction());

  //Fetch the action instance from the Factory based on the operation Config.
  SolrFaultTolerantAction cloneAction = SolrActionFactory.fetchActionToPerform(cloneCollectionConfig);

  //Execute the action
  cloneAction.executeAction();
```


#How to add a new Fault Tolerant Action

 - Write an Action class that extends SolrFaultTolerantAction and overrides the executeAction() method.

 - Use the ZkClient to access SolrCloud cluster/collection metadata and manipulate them as per your use case.

 - Add a string the represents this action in the FTActions.java

 - Change the SolrActionFactory.java to allow instantiation of the new Action.


#Known Issues
 - If you are running haft locally and replicating data between 2 solr clusters inside your datacenter, then it needs a way to map private ips to public names.
   This support is given only for ec2 currently. To overcome this, we recommend you to run haft inside a datacenter which can access the private ips
   of the solrcloud and zookeeper clusters.


# Contributors
 - Nitin Sharma (nitinssn@gmail.com)


# License

Apache License Version 2.0 http://www.apache.org/licenses/LICENSE-2.0


[SolrCloud]:https://cwiki.apache.org/confluence/display/solr/SolrCloud
[collections]:https://cwiki.apache.org/confluence/display/solr/Collections+API
[zk ensemble]:https://cwiki.apache.org/confluence/display/solr/Setting+Up+an+External+ZooKeeper+Ensemble
[configs]:https://cwiki.apache.org/confluence/display/solr/Using+ZooKeeper+to+Manage+Configuration+Files
