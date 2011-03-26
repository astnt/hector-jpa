/**
 * 
 */
package com.datastax.hectorjpa.store;

import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;

import com.datastax.hectorjpa.index.IndexDefinitions;

/**
 * @author Todd Nine
 * 
 */
public class CassandraFieldMetaData extends FieldMetaData {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7584475014214868856L;

	private IndexDefinitions indexDefinitions;
	
	protected CassandraFieldMetaData(String name, Class<?> type,
			ClassMetaData owner) {
		super(name, type, owner);
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
