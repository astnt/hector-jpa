/**
 * 
 */
package com.datastax.hectorjpa.store;

import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;

/**
 * @author Todd Nine
 * 
 */
public class CassandraFieldMetaData extends FieldMetaData {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7584475014214868856L;


  
  private boolean embeddedEntity;
  
  private boolean embeddedCollectionEntity;
  

	protected CassandraFieldMetaData(String name, Class<?> type,
			ClassMetaData owner) {
		super(name, type, owner);
	}


  /**
   * @return the embeddedEntity
   */
  public boolean isEmbeddedEntity() {
    return embeddedEntity;
  }


  /**
   * @param embeddedEntity the embeddedEntity to set
   */
  public void setEmbeddedEntity(boolean embeddedEntity) {
    this.embeddedEntity = embeddedEntity;
  }


  /**
   * @return the embeddedCollectionEntity
   */
  public boolean isEmbeddedCollectionEntity() {
    return embeddedCollectionEntity;
  }


  /**
   * @param embeddedCollectionEntity the embeddedCollectionEntity to set
   */
  public void setEmbeddedCollectionEntity(boolean embeddedCollectionEntity) {
    this.embeddedCollectionEntity = embeddedCollectionEntity;
  }

	
  
}
