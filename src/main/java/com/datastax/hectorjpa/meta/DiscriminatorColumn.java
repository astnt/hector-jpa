package com.datastax.hectorjpa.meta;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import com.datastax.hectorjpa.store.CassandraClassMetaData;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Class that always holds static information. Used for the placeholder column.
 * This column is required so we can differentiate between a tombstone row and a
 * row where only the id column was requested in the BitSet.  Also holds the subclass discriminator 
 * value so we can determine which class to load at runtime.
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class DiscriminatorColumn implements ObjectTypeColumnStrategy {

	public static final String DISCRIMINAATOR_COL = "jpacol";

	private static List<String> columns;

	private String value;


	public DiscriminatorColumn(String discriminatorValue) {
		columns = new ArrayList<String>();
		columns.add(DISCRIMINAATOR_COL);

		this.value = discriminatorValue;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(Mutator<byte[]> mutator, long clock, byte[] key,
			String cfName) {

		mutator.addInsertion(key, cfName, new HColumnImpl(DISCRIMINAATOR_COL,
				value, clock, StringSerializer.get(), StringSerializer.get()));

	}

	@Override
	public String getStoredType(Object rowKey, String cfName, Keyspace keyspace) {

		SliceQuery<byte[], String, byte[]> query = MappingUtils
				.buildSliceQuery(rowKey, columns, cfName, keyspace);

		QueryResult<ColumnSlice<String, byte[]>> result = query.execute();

		// only need to check > 0. If the entity wasn't tombstoned then we would
		// have loaded the static jpa marker column

		HColumn<String, byte[]> descrimValue = result.get().getColumnByName(
				DISCRIMINAATOR_COL);

		if (descrimValue == null) {
			return null;
		}

		return StringSerializer.get().fromBytes(descrimValue.getValue());
	}

	@Override
	public Class<?> getClass(String value, Class<?> candidate, MetaCache metaCache) {
	  CassandraClassMetaData meta = metaCache.getClassFromDiscriminator(value);
	  
	  if(meta == null){
	    return null;
	  }
	  
	  return meta.getDescribedType();
	  
	}

	
}