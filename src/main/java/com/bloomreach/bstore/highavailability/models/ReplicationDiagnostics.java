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

/**
 * Wrapper to hold information about Replication Diagnostics
 *
 * @author nitin
 * @since 3/27/14.
 */
public class ReplicationDiagnostics {
  private String reason;
  private String entity;
  private String timeElapsed;
  private String percentageComplete;
  private boolean isReplicating;
  private boolean failedReplication;

  public boolean isFailedReplication() {
    return failedReplication;
  }

  public void setFailedReplication(boolean failedReplication) {
    this.failedReplication = failedReplication;
  }

  public boolean isReplicating() {
    return isReplicating;
  }

  public void setReplicating(boolean isReplicating) {
    this.isReplicating = isReplicating;
  }

  public String getPercentageComplete() {
    return percentageComplete;
  }

  public void setPercentageComplete(String percentageComplete) {
    this.percentageComplete = percentageComplete;
  }

  public String getTimeElapsed() {
    return timeElapsed;
  }

  public void setTimeElapsed(String timeElapsed) {
    this.timeElapsed = timeElapsed;
  }

  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  public String getEntity() {
    return entity;
  }

  public void setEntity(String entity) {
    this.entity = entity;
  }
}
