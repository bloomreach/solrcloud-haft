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
import org.apache.zookeeper.data.Stat;
import org.codehaus.plexus.util.ExceptionUtils;

import java.util.List;

/**
 * Reads and Traverses Zookeeper Data. Fetches the raw bytes for every zk node and stores it inside the
 * {@link com.bloomreach.bstore.highavailability.zookeeper.ZkDataNode} data structure.
 * </p>
 *
 * Given a path, it recursively traverses the data and has an in-memory representation of the tree with
 * the path as the root.
 *
 * @author nitin
 * @since 01/06/2015
 */
public class ZookeeperDataTraverser {
  private static final Logger logger = Logger.getLogger(ZookeeperDataTraverser.class);
  /* Represents the zookeper Data Node */
  private final ZkDataNode znode;

  /* Represents the zookeper Handle */
  private ZooKeeper zk;

  /**
   * Constructs a Traverser with a zk Host and Root to traverse
   *
   * @param sourceZk The source Zookeper Host
   * @param path The root path of the zk to traverse from
   */
  public ZookeeperDataTraverser(String sourceZk, String path) {
    //Initialize ZkData Node based on path.
    ZkDataNode znode = new ZkDataNode(path);
    this.znode = znode;
    //Connects to the zk using the zk connection manager
    this.zk = ZKConnectionManager.connectToZookeeper(sourceZk);
  }

  /**
   * Triggers the recursive fetch of all the zk data for a given path
   *
   * @return znode Zookeeper Data Node that stores all zookeeper path data
   * @throws Exception
   */
  public ZkDataNode traverse() throws Exception {
    try {
      populate(znode);
    } catch (KeeperException e) {
      throw new ZkDataTraversalException(ExceptionUtils.getFullStackTrace(e));
    } catch (InterruptedException e) {
      throw new ZkDataTraversalException(ExceptionUtils.getFullStackTrace(e));
    }
    return znode;
  }

  /**
   * Recursively populates the ZkDataNode by reading all zk nodes
   * and its children.
   *
   * @param zkDataNode Zookeeper Data Node that stores all zookeeper path data
   * @throws Exception
   */
  public void populate(ZkDataNode zkDataNode) throws Exception {
    Stat stat = null;
    String path = zkDataNode.getFQPath();
    logger.info("Reading node " + path);
    byte[] data = zk.getData(path, false, stat);
    zkDataNode.setNodeData(data);
    List<String> subFolders = zk.getChildren(path, false);
    for (String folder : subFolders) {
      ZkDataNode childNode = new ZkDataNode(zkDataNode, folder);
      zkDataNode.addChild(childNode);
      populate(childNode);
    }
    return;
  }
}