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


  
  private boolean serializedEmbedded;
  

	protected CassandraFieldMetaData(String name, Class<?> type,
			ClassMetaData owner) {
		super(name, type, owner);
	}

	
  /**
   * @return the serializedEmbedded
   */
  public boolean isSerializedEmbedded() {
    return serializedEmbedded;
  }

  /**
   * @param serializedEmbedded the serializedEmbedded to set
   */
  public void setSerializedEmbedded(boolean serializedEmbedded) {
    this.serializedEmbedded = serializedEmbedded;
  }
	
}
