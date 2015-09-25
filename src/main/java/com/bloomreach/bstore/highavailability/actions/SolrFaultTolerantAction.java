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
import com.bloomreach.bstore.highavailability.zookeeper.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Generic SolrAction Class. Specific implemenations should override
 * the executeAction to do their operations.
 * @autho nitin
 * @since 2/19/14.
 */
public abstract class SolrFaultTolerantAction {
  protected static final Logger logger = Logger.getLogger(SolrFaultTolerantAction.class);

  protected OperationConfig config;
  protected ZkClient sourceZKClient;

  /**
   * Initialize the zk Config
   * @param config
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  public SolrFaultTolerantAction(OperationConfig config) throws IOException, InterruptedException, KeeperException, CollectionNotFoundException {
    this.config = config;
    if (config.getZkHost() != null) {
      sourceZKClient = new ZkClient(config.getZkHost());
    }
  }

  /**
   * Generic wrapper which will be implemented by the specific Fault Tolerant
   * Action.
   *
   * @throws Exception
   */
  public abstract void executeAction() throws Exception;
}
