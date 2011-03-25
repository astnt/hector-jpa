package com.datastax.hectorjpa.meta;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.openjpa.meta.ClassMetaData;

import com.datastax.hectorjpa.store.CassandraClassMetaData;
import com.datastax.hectorjpa.store.EntityFacade;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Cache for holding all meta data
 * 
 * @author Todd Nine
 * 
 */
public class MetaCache {

	private final ConcurrentMap<ClassMetaData, EntityFacade> metaData = new ConcurrentHashMap<ClassMetaData, EntityFacade>();

	private final ConcurrentMap<String, CassandraClassMetaData> discriminators = new ConcurrentHashMap<String, CassandraClassMetaData>();

	private final MappingUtils mappingUtils;

	/**
	 * Create a new meta cache for classes
	 * 
	 * @param mappingUtils
	 */
	public MetaCache(MappingUtils mappingUtils) {
		this.mappingUtils = mappingUtils;
	}

	/**
	 * Get the entity facade for this class. If it does not exist it is created
	 * and added to the cache
	 * 
	 * @param meta
	 * @return
	 */
	public EntityFacade getFacade(ClassMetaData meta) {

		CassandraClassMetaData cassMeta = (CassandraClassMetaData) meta;

		EntityFacade facade = metaData.get(cassMeta);

		if (facade != null) {
			return facade;
		}

		facade = new EntityFacade(cassMeta, mappingUtils);

		metaData.putIfAbsent(cassMeta, facade);

		String discriminatorValue = cassMeta.getDiscriminatorColumn();

		discriminators.putIfAbsent(discriminatorValue, cassMeta);

		return facade;

	}

	/**
	 * Get the class name from the discriminator string.  Null if one doesn't exist
	 * 
	 * @param discriminator
	 * @return
	 */
	public CassandraClassMetaData getClassFromDiscriminator(String discriminator) {
		return discriminators.get(discriminator);
	}

}
