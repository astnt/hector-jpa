package com.datastax.hectorjpa.meta;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.FieldMetaData;

import com.datastax.hectorjpa.service.IndexQueue;

/**
 * Class for serializing big decimals into 2 columns. The first is the field
 * value as an unscaled bigInt, then second is in the format of <fieldName>Scale
 * with the scale
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class BigDecimalColumnField extends StringColumnField {

  private static final String SUFFIX = "Scale";

  protected static final StringSerializer STR_SER = StringSerializer.get();
  protected static final BigIntegerSerializer BIGINT_SER = BigIntegerSerializer
      .get();
  protected static final IntegerSerializer INT_SER = IntegerSerializer.get();

  private String scaleColumnName;

  public BigDecimalColumnField(FieldMetaData fmd) {
    this(fmd.getIndex(), fmd.getName());
  }

  public BigDecimalColumnField(int fieldId, String fieldName) {
    super(fieldId, fieldName);
    this.scaleColumnName = fieldName + SUFFIX;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  @Override
  public void addFieldNames(List<String> fields) {
    fields.add(name);
    fields.add(scaleColumnName);
  }

  /**
   * Adds this field to the mutation with the given clock
   * 
   * @param stateManager
   * @param mutator
   * @param clock
   * @param key
   *          The row key
   * @param cfName
   *          the column family name
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void addField(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, byte[] key, String cfName,
      IndexQueue queue) {

    Object value = stateManager.fetch(fieldId);

    if (value == null) {
      mutator.addDeletion(key, cfName, name, STR_SER, clock);
      mutator.addDeletion(key, cfName, scaleColumnName, STR_SER, clock);
      return;
    }

    mutator.addInsertion(key, cfName, new HColumnImpl(name,
        ((BigDecimal) value).unscaledValue(), clock, STR_SER, BIGINT_SER));
    mutator.addInsertion(key, cfName, new HColumnImpl(scaleColumnName,
        ((BigDecimal) value).scale(), clock, STR_SER, INT_SER));

  }

  /**
   * Read the field from the query result into the opject within the state
   * manager.
   * 
   * @param stateManager
   * @param result
   * @return True if the field was loaded. False otherwise
   */
  public boolean readField(OpenJPAStateManager stateManager,
      QueryResult<ColumnSlice<String, byte[]>> result) {

    HColumn<String, byte[]> bigIntCol = result.get().getColumnByName(name);
    HColumn<String, byte[]> scaleCol = result.get().getColumnByName(
        scaleColumnName);

    if (bigIntCol == null || scaleCol == null) {
      stateManager.store(fieldId, null);
      return false;
    }

    BigInteger unscaled = BIGINT_SER.fromByteBuffer(bigIntCol.getValueBytes());

    int scale = INT_SER.fromByteBuffer(scaleCol.getValueBytes());

    stateManager.store(fieldId, new BigDecimal(unscaled, scale));

    return true;
  }

}
