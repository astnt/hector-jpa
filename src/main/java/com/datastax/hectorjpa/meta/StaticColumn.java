package com.datastax.hectorjpa.meta;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;

/**
 * Class that always holds static information. Used for the placeholder column.
 * This column is required so we can differentiate between a tombstone row and a
 * row where only the id column was requested in the BitSet
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class StaticColumn<V> extends ColumnField<byte[]> {

  public static final int HOLDER_FIELD_ID = -1;

  public static final String EMPTY_COL = "jpaholder";

  private static final byte[] EMPTY_VAL = new byte[] { 0 };

  public StaticColumn() {
    super(HOLDER_FIELD_ID, EMPTY_COL, BytesArraySerializer.get());
  }



  /*
   * (non-Javadoc)
   * 
   * @see
   * com.datastax.hectorjpa.meta.ColumnField#addField(org.apache.openjpa.kernel
   * .OpenJPAStateManager, me.prettyprint.hector.api.mutation.Mutator, long,
   * byte[], java.lang.String)
   */
  @Override
  public void addField(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, byte[] key, String cfName) {

    mutator.addInsertion(key, cfName, new HColumnImpl(EMPTY_COL, EMPTY_VAL,
        clock, StringSerializer.get(), serializer));

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.datastax.hectorjpa.meta.ColumnField#readField(org.apache.openjpa.kernel
   * .OpenJPAStateManager, me.prettyprint.hector.api.query.QueryResult)
   */
  @Override
  public void readField(OpenJPAStateManager stateManager,
      QueryResult<ColumnSlice<String, byte[]>> result) {

    // no op
  }

}