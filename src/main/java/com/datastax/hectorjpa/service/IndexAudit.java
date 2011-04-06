package com.datastax.hectorjpa.service;

import java.util.Arrays;

import me.prettyprint.hector.api.beans.DynamicComposite;

/**
 * Bean to hold audit information for an index
 * @author Todd Nine
 *
 */
public class IndexAudit {

  private byte[] readRowKey;
  private byte[] idRowKey;
  private DynamicComposite columnId;
  private long clock;
  private String columnFamily;
  private boolean biDirectional;
  
  /**
   * 
   * @param readRowKey The row key for the index users read from format of <fields>+<order>+id
   * @param idRowKey The row key for the columns in the format id+<fields>+<order>
   * @param columnId The column id to use in the range scan
   * @param clock The clock time to use for all update operations
   */
  public IndexAudit(byte[] readRowKey, byte[] idRowKey, DynamicComposite columnId, long clock, String columnFamily, boolean biDirectional) {
    super();
    this.readRowKey = readRowKey;
    this.idRowKey = idRowKey;
    this.columnId = columnId;
    this.columnFamily = columnFamily;
    this.clock = clock;
    this.biDirectional = biDirectional;
  }

  /**
   * @return the readRowKey
   */
  public byte[] getReadRowKey() {
    return readRowKey;
  }

  /**
   * @return the idRowKey
   */
  public byte[] getIdRowKey() {
    return idRowKey;
  }

  /**
   * @return the columnId
   */
  public DynamicComposite getColumnId() {
    return columnId;
  }

  /**
   * @return the clock
   */
  public long getClock() {
    return clock;
  }

  /**
   * @return the columnFamily
   */
  public String getColumnFamily() {
    return columnFamily;
  }

  /**
   * @return the hasReverse
   */
  public boolean isBiDirectional() {
    return biDirectional;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (clock ^ (clock >>> 32));
    result = prime * result
        + ((columnFamily == null) ? 0 : columnFamily.hashCode());
    result = prime * result + ((columnId == null) ? 0 : columnId.hashCode());
    result = prime * result + Arrays.hashCode(idRowKey);
    result = prime * result + Arrays.hashCode(readRowKey);
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof IndexAudit))
      return false;
    IndexAudit other = (IndexAudit) obj;
    if (clock != other.clock)
      return false;
    if (columnFamily == null) {
      if (other.columnFamily != null)
        return false;
    } else if (!columnFamily.equals(other.columnFamily))
      return false;
    if (columnId == null) {
      if (other.columnId != null)
        return false;
    } else if (!columnId.equals(other.columnId))
      return false;
    if (!Arrays.equals(idRowKey, other.idRowKey))
      return false;
    if (!Arrays.equals(readRowKey, other.readRowKey))
      return false;
    return true;
  }

 
}
