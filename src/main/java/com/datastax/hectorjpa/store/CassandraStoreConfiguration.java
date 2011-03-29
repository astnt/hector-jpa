package com.datastax.hectorjpa.store;

import org.apache.openjpa.conf.OpenJPAConfigurationImpl;
import org.apache.openjpa.lib.conf.ProductDerivations;
import org.apache.openjpa.meta.MetaDataFactory;
import org.apache.openjpa.meta.MetaDataRepository;

import com.datastax.hectorjpa.meta.MetaCache;

public class CassandraStoreConfiguration extends OpenJPAConfigurationImpl {

	private MetaCache metaCache;

	public CassandraStoreConfiguration() {
		super(false, false);

		// override the default and the current value of lock manager plugin
		// from our superclass to use the single-jvm lock manager
		lockManagerPlugin.setDefault("version");
		lockManagerPlugin.setString("version");
		addString("me.prettyprint.hom.classpathPrefix");
		addString("me.prettyprint.hom.keyspace");
		addString("me.prettyprint.hom.clusterName");
		addString("me.prettyprint.hom.hostList");

		// TODO map our metadata plugin parser factory for parsing index
		// annotations
		// metaFactoryPlugin.setClassName(CassandraPersistenceMappingParser.class.getName());

		ProductDerivations.beforeConfigurationLoad(this);
		loadGlobals();

		this.metaCache = new MetaCache();

	}

	

	/**
	 * @return the cache
	 */
	public MetaCache getMetaCache() {
		return metaCache;
	}


}
