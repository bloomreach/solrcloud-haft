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
 *
 */
package com.bloomreach.bstore.highavailability.actions;

/**
 * Enum to Store the various types of {@link com.bloomreach.bstore.highavailability.actions.SolrFaultTolerantAction}
 *
 * @author nitin
 * @since 2/20/14.
 */
public enum FTActions {
  DELETE("delete"), CLONE("clone"), CLONE_ZK("clone_zk"),
  CLONE_ALIAS("clone_alias"), SMOKE_TEST("smoke_test");

  private String action;

  private FTActions(String actions) {
    action = actions;
  }

  public String getAction() {
    return action;
  }
}
