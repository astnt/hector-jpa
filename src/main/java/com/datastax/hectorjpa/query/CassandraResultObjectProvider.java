package com.datastax.hectorjpa.query;

import java.util.Iterator;
import java.util.Set;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.kernel.FetchConfiguration;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.lib.rop.ResultObjectProvider;

import com.datastax.hectorjpa.meta.key.KeyStrategy;
import com.datastax.hectorjpa.store.CassandraClassMetaData;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * 
 * @author Todd Nine
 * 
 */
public class CassandraResultObjectProvider implements ResultObjectProvider {

	
	private final StoreContext ctx;
	private final CassandraClassMetaData classMeta;
	private final FetchConfiguration fetchConfig;
  private final KeyStrategy keyStrategy;
  
  
	private Iterator<DynamicComposite> iterator;
	private Set<DynamicComposite> results;
	
	public CassandraResultObjectProvider(Set<DynamicComposite> results,
			StoreContext ctx, FetchConfiguration fetchConfig,
			CassandraClassMetaData classMeta) {
		this.results = results;
		this.ctx = ctx;
		this.classMeta = classMeta;
		this.fetchConfig = fetchConfig;

		keyStrategy = MappingUtils.getKeyStrategy(classMeta);
	}

	@Override
	public boolean supportsRandomAccess() {
		return false;
	}

	@Override
	public void open() throws Exception {
		this.iterator = results.iterator();

	}

	@Override
	public Object getResultObject() throws Exception {

		DynamicComposite current = iterator.next();

		int length = current.getComponents().size();

		Object id = keyStrategy.getInstance(current.getComponent(length -1).getBytes());

		Object jpaId = ctx.newObjectId(classMeta.getDescribedType(), id);

		return ctx.find(jpaId, fetchConfig, null, null, 0);
	}

	@Override
	public boolean next() throws Exception {
		return iterator.hasNext();
	}

	@Override
	public boolean absolute(int pos) throws Exception {
		return false;

	}

	@Override
	public int size() throws Exception {
		return results.size();
	}

	@Override
	public void reset() throws Exception {
		this.iterator = results.iterator();
	}

	@Override
	public void close() throws Exception {
		// no op

	}

	@Override
	public void handleCheckedException(Exception e) {
		// no op

	}

}
