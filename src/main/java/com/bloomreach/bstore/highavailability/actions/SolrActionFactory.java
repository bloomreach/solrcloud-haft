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

/**
 * Factory to instantiate the right Fault Tolerance Action
 * @author nitin
 * @since 2/20/14.
 */
public class SolrActionFactory {

  /**
   * Given an action, returns an {@link com.bloomreach.bstore.highavailability.actions.SolrFaultTolerantAction} object
   *
   * @return {@link com.bloomreach.bstore.highavailability.actions.SolrFaultTolerantAction} that represents the needed action
   */
  public static SolrFaultTolerantAction fetchActionToPerform(OperationConfig config) throws Exception {
    SolrFaultTolerantAction solrAction = null;

     if (config.getAction().equals(FTActions.DELETE.getAction())) {
      solrAction = new SolrDeleteCollectionAction(config);
    } else if (config.getAction().equals(FTActions.CLONE.getAction())) {
      solrAction = new CloneCollectionsAction(config);
    } else if (config.getAction().equals(FTActions.CLONE_ALIAS.getAction())) {
      solrAction = new CloneAliasesAction(config);
    } else if (config.getAction().equals(FTActions.SMOKE_TEST.getAction())) {
      solrAction = new SmokeTestClusterAction(config);
    }  else  if (config.getAction().equals(FTActions.CLONE_ZK.getAction())) {
       solrAction = new CloneZkConfigsAction(config);
     }

    if (solrAction == null) {
      throw new IllegalArgumentException("Valid Action does not exist for " + config.getAction());
    }
    return solrAction;
  }

}
