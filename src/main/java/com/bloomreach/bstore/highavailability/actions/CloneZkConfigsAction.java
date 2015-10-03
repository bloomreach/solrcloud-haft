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

import com.bloomreach.bstore.highavailability.exceptions.CollectionNotFoundException;
import com.bloomreach.bstore.highavailability.exceptions.ZkDataTraversalException;
import com.bloomreach.bstore.highavailability.zookeeper.ZkDataNode;
import com.bloomreach.bstore.highavailability.zookeeper.ZookeeperDataReplicator;
import com.bloomreach.bstore.highavailability.zookeeper.ZookeeperDataTraverser;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Clones all Zookeeper Configs from One Zk to another. It is an implementation of the
 * {@link com.bloomreach.bstore.highavailability.actions.SolrFaultTolerantAction}. Its behavior is
 * controlled by the generic {@link com.bloomreach.bstore.highavailability.actions.OperationConfig}.
 * </p>
 *
 * It leverages the
 * {@link com.bloomreach.bstore.highavailability.zookeeper.ZookeeperDataTraverser} to collect
 * all zk file level information under the given root.
 * </p>
 *
 * Once it collects the data, it uses the
 * {@link com.bloomreach.bstore.highavailability.zookeeper.ZookeeperDataReplicator} class
 * to replicate the data on the destination zookeeper.
 *
 *
 * @author nitin
 * @since 3/25/14.
 */
public class CloneZkConfigsAction extends SolrFaultTolerantAction {
  private static final String CONFIG_PATH = "/configs";

  public CloneZkConfigsAction(OperationConfig config) throws IOException, InterruptedException, KeeperException, CollectionNotFoundException {
    super(config);
  }

  @Override
  public void executeAction() throws Exception {
    String sourceZk = config.getZkHost();
    String destinationZk = config.getDestinationZkHost();

    ZkDataNode rootNode;
    //Traverse the data
    try {
      rootNode = new ZookeeperDataTraverser(sourceZk, CONFIG_PATH).traverse();
    } catch (Exception e) {
      logger.info("Encountered Exception.." + ExceptionUtils.getFullStackTrace(e));
      throw new ZkDataTraversalException("Encountered Exception.." + ExceptionUtils.getFullStackTrace(e));
    }

    //Replicate the data
    ZookeeperDataReplicator replicator = new ZookeeperDataReplicator(destinationZk, CONFIG_PATH, rootNode);
    replicator.replicate();
  }
}
