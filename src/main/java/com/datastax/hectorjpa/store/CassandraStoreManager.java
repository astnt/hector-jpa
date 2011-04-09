package com.datastax.hectorjpa.store;

import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.openjpa.abstractstore.AbstractStoreManager;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.kernel.FetchConfiguration;
import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.kernel.PCState;
import org.apache.openjpa.kernel.QueryLanguages;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.kernel.exps.ExpressionParser;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.util.OpenJPAId;
import org.apache.openjpa.util.UnsupportedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.query.CassandraStoreQuery;
import com.datastax.hectorjpa.service.IndexQueue;

public class CassandraStoreManager extends AbstractStoreManager {

	private static Logger log = LoggerFactory
			.getLogger(CassandraStoreManager.class);

	private CassandraStore cassandraStore;
	
	private CassandraStoreConfiguration config;

	
	
	public CassandraStoreManager() {
    super();
 
  }

  @Override
	public ResultObjectProvider executeExtent(ClassMetaData cMetaData,
			boolean useSubClasses, FetchConfiguration fetchConfiguration) {
		if (log.isDebugEnabled()) {
			log.debug(
					"in executeExtent with ClassMetaData {}: useSubClasses: {} and fetchConfiguration: {}",
					new Object[] { cMetaData, useSubClasses, fetchConfiguration });
		}

	
		return null;
	}

	@Override
	public StoreQuery newQuery(String language) {
		ExpressionParser ep = QueryLanguages.parserForLanguage(language);

		if (ep == null) {
			throw new UnsupportedException(language);
		}

		CassandraStoreConfiguration conf = ((CassandraStoreConfiguration) getContext()
				.getConfiguration());

		return new CassandraStoreQuery(ep, conf);
	}

	

	@SuppressWarnings("unchecked")
	@Override
	protected Collection flush(Collection pNew, Collection pNewUpdated,
			Collection pNewFlushedDeleted, Collection pDirty,
			Collection pDeleted) {
	
	  
		long clock = config.getKeyspace().createClock();

		Mutator<?> mutator = createMutator();
		
		IndexQueue queue = new IndexQueue();

		writeEntities(pNew, mutator, clock, queue);
		writeEntities(pNewUpdated, mutator, clock, queue);
		writeEntities(pDirty, mutator, clock, queue);

		deleteEntities(pNewFlushedDeleted, mutator, clock, queue);
		deleteEntities(pDeleted, mutator, clock, queue);

		mutator.execute();
		//now that the mutator has returned.  Execute index cleanup
		
		queue.writeAudits(config.getIndexingService());
		

		return null;
	}

	/**
	 * Write all entities in the collection
	 * 
	 * @param writes
	 * @param mutator
	 * @param clock
	 * @param queue 
	 */
	private void writeEntities(Collection writes, Mutator mutator, long clock, IndexQueue queue) {

		OpenJPAStateManager sm = null;

		for (Iterator itr = writes.iterator(); itr.hasNext();) {
			sm = (OpenJPAStateManager) itr.next();
			cassandraStore.storeObject(mutator, sm, sm.getDirty(), clock, queue);
		}

	}

	/**
	 * Delete all entities in the collection
	 * 
	 * @param deletes
	 * @param mutator
	 * @param clock
	 * @param queue 
	 */
	private void deleteEntities(Collection deletes, Mutator mutator, long clock, IndexQueue queue) {
		for (Iterator itr = deletes.iterator(); itr.hasNext();) {
			// create new object data for instance
			OpenJPAStateManager sm = (OpenJPAStateManager) itr.next();
			cassandraStore.removeObject(mutator, sm, clock, queue);

		}
	}

	@Override
	public boolean initialize(OpenJPAStateManager stateManager,
			PCState pcState, FetchConfiguration fetchConfiguration, Object obj) {

		// if it's an abstract type, we couldn't find it in the datastore
		// because getManagedType returned null
		// and the framework set the class type to the type the user queried.
		// Just return false because it doesn't exist
		Class<?> type = cassandraStore.getDataStoreId(stateManager.getId(), this.getContext());

		if(type == null){
			return false;
		}
		
		Class<?> requestedType = ((OpenJPAId)stateManager.getId()).getType();
		
		/**
		 * Requested class is a superclass of the stored type.
		 */
		if(!requestedType.isAssignableFrom(type)){
			return false;
		}

		log.debug("In initialize operation...");
		stateManager.initialize(type, pcState);
		stateManager.load(fetchConfiguration);
		
		return true;

	}

	@Override
	public boolean load(OpenJPAStateManager stateManager, BitSet fields,
			FetchConfiguration fetch, int lockLevel, Object edata) {
		log.debug("In load operation...");
		// return cassandraStore.getObject(stateManager, fields);

		// load is called to fill in additional information not retrieved from
		// initialize call above
		return cassandraStore.getObject(stateManager, fields);

	}

	@Override
	public boolean exists(OpenJPAStateManager stateManager, Object edata) {
		log.debug("In CSM.exists()");
		return cassandraStore.exists(stateManager);
	}

	@Override
	public boolean isCached(List<Object> oids, BitSet edata) {
		log.debug("In CSM.isCached()");
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int compareVersion(OpenJPAStateManager state, Object v1, Object v2) {
		log.debug("in CSM.compareVersion");
		return super.compareVersion(state, v1, v2);
	}

	@Override
	public boolean syncVersion(OpenJPAStateManager sm, Object edata) {
		// TODO Auto-generated method stub
		log.debug("in CSM.syncVersion");
		return super.syncVersion(sm, edata);
	}

	@Override
	protected void open() {
		this.config = (CassandraStoreConfiguration) ctx.getConfiguration();
	
		cassandraStore = new CassandraStore(config);
		
		log.debug("in CSM.open()");
	}

	protected Collection getUnsupportedOptions() {
		Collection c = super.getUnsupportedOptions();

		// remove options we do support but the abstract store doesn't
		// c.remove(OpenJPAConfiguration.OPTION_ID_DATASTORE);
		// c.remove(OpenJPAConfiguration.OPTION_OPTIMISTIC);

		// and add some that we don't support but the abstract store does
		// TODO take these out one by one
		 c.add(OpenJPAConfiguration.OPTION_EMBEDDED_RELATION);
		 c.add(OpenJPAConfiguration.OPTION_EMBEDDED_COLLECTION_RELATION);
		 c.add(OpenJPAConfiguration.OPTION_EMBEDDED_MAP_RELATION);
		return c;
	}

	@Override
	protected OpenJPAConfiguration newConfiguration() {

		CassandraStoreConfiguration conf = new CassandraStoreConfiguration();

		if (log.isDebugEnabled()) {
			log.debug("In newConfiguration with conf: {}",
					conf.toProperties(true));
		}

		return conf;
	}
	
	/**
	 * Create a new mutator
	 * @return
	 */
	private Mutator<byte[]> createMutator(){
	  return new MutatorImpl<byte[]>(this.config.getKeyspace(), BytesArraySerializer.get());
	}

}

