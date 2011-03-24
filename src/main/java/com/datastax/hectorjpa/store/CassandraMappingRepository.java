/**
 * 
 */
package com.datastax.hectorjpa.store;

import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.MetaDataRepository;

/**
 * The cassandra mapping repository
 * @author Todd Nine
 *
 */
public class CassandraMappingRepository extends MetaDataRepository {

  /**
   * 
   */
  private static final long serialVersionUID = -5608784558716855684L;

  /* (non-Javadoc)
   * @see org.apache.openjpa.meta.MetaDataRepository#getOrderByField(org.apache.openjpa.meta.ClassMetaData, java.lang.String)
   */
  @Override
  public FieldMetaData getOrderByField(ClassMetaData meta, String orderBy) {
    // TODO Auto-generated method stub
    return super.getOrderByField(meta, orderBy);
  }

  
  
}
