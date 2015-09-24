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

import java.util.LinkedList;
import java.util.List;

/**
 * Represents the in-memory version of a {@link org.apache.zookeeper} data.
 * </p>
 *
 * It stores the raw byte content for this node, a pointer to it's parent,
 * the label for this node and a {@link java.util.List} of all its children of type
 * {@link com.bloomreach.bstore.highavailability.zookeeper.ZkDataNode}
 *
 * @author nitin
 * @since 01/06/2015
 */
public class ZkDataNode {
  //Represents the data in the node
  private byte[] nodeData;

  //Represents the myId for this current Node
  private String myId;

  //We need to hold the parent information to append the myId and construct the absolute myId
  private ZkDataNode parent;

  //Storing all my child info. This is needed to recursively iterate.
  private final List<ZkDataNode> allChildren;

  /**
   * Constructs a ZkDataNode
   *
   * @param parent The parent ZkData Node
   * @param myId The current path's id
   */
  public ZkDataNode(ZkDataNode parent, String myId) {
    this.parent = parent;
    this.myId = myId;
    this.nodeData = null;
    allChildren = new LinkedList<ZkDataNode>();
  }

  /**
   * Constructs a root ZkDataNode
   *
   * @param myId The current path's id
   */
  public ZkDataNode(String myId) {
    this(null, myId);
  }

  /**
   * Add a ZkData Node to the child.
   *
   * @param child The parent ZkData Node
=  */
  public void addChild(ZkDataNode child) {
    allChildren.add(child);
  }

  public String getMyId() {
    return myId;
  }

  public void setMyId(String myId) {
    this.myId = myId;
  }

  public ZkDataNode getParent() {
    return parent;
  }

  public void setParent(ZkDataNode parent) {
    this.parent = parent;
  }


  public List<ZkDataNode> getAllChildren() {
    return allChildren;
  }

  /**
   * Fetch the fully qualified path for this zk data Node
   * @return
   */
  public String getFQPath() {
    if (parent == null) {
      return myId;
    } else {
      return parent.getFQPath() + "/" + myId;
    }
  }

  public void setNodeData(byte[] data) {
    this.nodeData = data;
  }

  public byte[] getNodeData() {
    return nodeData;
  }
}
