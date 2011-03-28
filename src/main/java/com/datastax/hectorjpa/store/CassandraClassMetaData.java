/**
 * 
 */
package com.datastax.hectorjpa.store;

import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.MetaDataRepository;
import org.apache.openjpa.meta.ValueMetaData;

import com.datastax.hectorjpa.index.IndexDefinitions;

/**
 * Meta data for classes specific to Cassandra
 * 
 * @author Todd Nine
 * 
 */
public class CassandraClassMetaData extends ClassMetaData {

  /**
	 * 
	 */
  private static final long serialVersionUID = -7584475014214868856L;

  private String discriminatorColumn;

  private String columnFamily;

  private IndexDefinitions indexDefinitions;

  protected CassandraClassMetaData(Class<?> type, MetaDataRepository repos) {
    super(type, repos);
  }

  public CassandraClassMetaData(ValueMetaData owner) {
    super(owner);
  }

  /**
   * Get the discriminator value. Null if one hasn't been set
   * 
   * @return
   */
  public String getDiscriminatorColumn() {
    return discriminatorColumn;
  }

  /**
   * Set the discriminator column
   * 
   * @param discriminatorColumn
   */
  public void setDiscriminatorColumn(String discriminatorColumn) {
    this.discriminatorColumn = discriminatorColumn;
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public void setColumnFamily(String columnFamily) {
    this.columnFamily = columnFamily;
  }

  /**
   * @return
   */
  public IndexDefinitions getIndexDefinitions() {
    return indexDefinitions;
  }

  /**
   * @param indexDefinitions
   */
  public void setIndexDefinitions(IndexDefinitions indexDefinitions) {
    this.indexDefinitions = indexDefinitions;
  }

}
