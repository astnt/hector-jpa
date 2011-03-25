/**
 * 
 */
package com.datastax.hectorjpa.store;

import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.MetaDataRepository;
import org.apache.openjpa.meta.ValueMetaData;

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
	
	private String discriminatorValue;

	protected CassandraClassMetaData(Class<?> type, MetaDataRepository repos) {
		super(type, repos);
	}

	public CassandraClassMetaData(ValueMetaData owner) {
		super(owner);
	}

	/**
	 * Get the discriminator value.  Null if one hasn't been set
	 * @return
	 */
	public String getDiscriminatorColumn() {
		return discriminatorValue;
	}

	/**
	 * Set the discriminator column
	 * @param discriminatorColumn
	 */
	public void setDiscriminatorColumn(String discriminatorColumn) {
		this.discriminatorValue = discriminatorColumn;
	}

}
