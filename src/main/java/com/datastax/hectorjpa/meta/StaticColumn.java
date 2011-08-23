package com.datastax.hectorjpa.meta;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Class that always holds static information. Used for the placeholder column.
 * This column is required so we can differentiate between a tombstone row and a
 * row where only the id column was requested in the BitSet
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class StaticColumn implements ObjectTypeColumnStrategy {
	
	public static final String EMPTY_COL = "jpacol";

	private static final String FOUND = "FOUND"; 
	
	private static final byte[] EMPTY_VAL = new byte[] { 0 };

	private static List<String> columns;


	public StaticColumn() {
		columns = new ArrayList<String>();
		columns.add(EMPTY_COL);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void write(Mutator<byte[]> mutator, long clock, byte[] key,
			String cfName) {

		mutator.addInsertion(key, cfName, new HColumnImpl(EMPTY_COL, EMPTY_VAL,
				clock, StringSerializer.get(), BytesArraySerializer.get()));

	}

	@Override
	public String getStoredType(byte[] rowKey, String cfName, Keyspace keyspace) {
		

		SliceQuery<byte[], String, byte[]> query = MappingUtils
				.buildSliceQuery(rowKey, columns, cfName, keyspace);

		QueryResult<ColumnSlice<String, byte[]>> result = query.execute();

		// only need to check > 0. If the entity wasn't tombstoned then we would
		// have loaded the static jpa marker column

		if (result.get().getColumns().size() > 0) {
			return FOUND;
		}

		return null;
	}

	@Override
	public Class<?> getClass(String value, Class<?> candidate, MetaCache metaCache) {
		return candidate;
	}

	@Override
	public String getColumnName() {
		return EMPTY_COL;
	}
	
}