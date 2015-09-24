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

import com.bloomreach.bstore.highavailability.exceptions.ZkDataTraversalException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.codehaus.plexus.util.ExceptionUtils;


/**
 * Zookeeper Data Replicator. Given a copy of {@link com.bloomreach.bstore.highavailability.zookeeper.ZkDataNode}
 * it iterates over it and constructs the same structure on the given destination {@link org.apache.zookeeper}
 * destination end point.
 *
 * @author nitin
 * @since 01/06/2015
 */
public class ZookeeperDataReplicator {
  private static Logger logger = Logger.getLogger(ZookeeperDataReplicator.class);

  /* Represents the zookeper Data Node */
  private ZkDataNode sourceZkDataNode;

  /* Represents the root Zk Clone Path */
  private String sourceCloneZkPath;

  /* Represents the zookeper Handle */
  private ZooKeeper zkHandle;

  /**
   * Constructs a Zookeeper Data Replicators with destination Zookeeper Host, the root path to replicate
   * and the zkData to copy from
   *
   * @param destinationZkServer The destination Zookeeper Host
   * @param rootReplicatePath The root path to replicate
   * @param sourceZkData The Zookeeper data to copy from
   */
  public ZookeeperDataReplicator(String destinationZkServer, String rootReplicatePath, ZkDataNode sourceZkData) {
    this.sourceZkDataNode = sourceZkData;
    this.sourceCloneZkPath = rootReplicatePath;
    this.zkHandle = ZKConnectionManager.connectToZookeeper(destinationZkServer);
  }

  /**
   * Replicate data to Destination Zk based on the incoming zk data structure
   * Refer {@link #ZookeeperDataReplicator(String destinationZkServer, String rootReplicatePath, ZkDataNode sourceZkData)}
   */
  public void replicate() throws ZkDataTraversalException {
    try {
      ZKAccessUtils.validateAndCreateZkPath(zkHandle, sourceCloneZkPath, null);
      writeAndFlushData(sourceZkDataNode);
    } catch (Exception e) {
      throw new ZkDataTraversalException(ExceptionUtils.getFullStackTrace(e));
    }
  }


  /**
   * Create Path and Flush out Data for the corresponding Zookeeper node and its children.
   *
   * @param node The Zookeeper data to copy from
   * @throws KeeperException
   * @throws InterruptedException
   */
  private void writeAndFlushData(ZkDataNode node) throws KeeperException, InterruptedException {
    String path = node.getFQPath();
    if (ZKAccessUtils.zkPathExists(zkHandle, path)) {
      logger.info("Path Exists: Setting data...  " + path);
      ZKAccessUtils.setDataOnZkNode(zkHandle, path, node.getNodeData());
    } else {
      logger.info("Path does not exist. Creating now... " + path);
      ZKAccessUtils.validateAndCreateZkPath(zkHandle, path, node.getNodeData());
    }
    for (ZkDataNode child : node.getAllChildren()) {
      writeAndFlushData(child);
    }
  }
}
