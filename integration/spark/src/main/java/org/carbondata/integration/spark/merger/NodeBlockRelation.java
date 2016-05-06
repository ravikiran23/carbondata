package org.carbondata.integration.spark.merger;

import org.carbondata.core.carbon.datastore.block.TableBlockInfo;

/**
 *
 */
public class NodeBlockRelation implements Comparable<NodeBlockRelation> {

  private final TableBlockInfo block;
  private final String node;

  public NodeBlockRelation(TableBlockInfo block, String node) {
    this.block = block;
    this.node = node;

  }

  public TableBlockInfo getBlock() {
    return block;
  }

  public String getNode() {
    return node;
  }

  @Override public int compareTo(NodeBlockRelation obj) {
    return this.getNode().compareTo(obj.getNode());
  }
}
