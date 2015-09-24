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
package com.bloomreach.bstore.highavailability.models;

import cascading.util.Pair;

import java.util.Map;

/**
 * Encapsulates Cross Zk Cluster mappings, state and version mappings
 *
 * @author nitin
 * @since 3/25/14.
 */
public class ZkBridgeData {
  /* For every collection map a source core to destination core across clusters*/
  private Map<String, Map<String, Pair<CorePairMetadata, CorePairMetadata>>> sourceToDestionationPeerMapping;

  public Map<String, Map<String, Pair<CorePairMetadata, CorePairMetadata>>> getSourceToDestionationPeerMapping() {
    return sourceToDestionationPeerMapping;
  }

  public void setSourceToDestionationPeerMapping(Map<String, Map<String, Pair<CorePairMetadata, CorePairMetadata>>> sourceToDestionationPeerMapping) {
    this.sourceToDestionationPeerMapping = sourceToDestionationPeerMapping;
  }
}
