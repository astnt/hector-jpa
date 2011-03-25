/**
 * 
 */
package com.datastax.hectorjpa.store;

import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.MetaDataRepository;

/**
 * Creates custom class meta data specific for cassandra use
 * 
 * @author Todd Nine
 * 
 */
public class CassandraMetaDataRepository extends MetaDataRepository {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3365273445986172320L;

	@Override
	protected ClassMetaData newClassMetaData(Class<?> type) {
		return new CassandraClassMetaData(type, this);
	}

	@Override
	protected FieldMetaData newFieldMetaData(String name, Class<?> type,
			ClassMetaData owner) {
		return new CassandraFieldMetaData(name, type, owner);
	}

}
