/**
 * 
 */
package com.datastax.hectorjpa.meta;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.model.thrift.ThriftSliceQuery;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.utils.ByteBufferOutputStream;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.MetaDataException;

import com.datastax.hectorjpa.index.FieldOrder;
import com.datastax.hectorjpa.index.IndexDefinition;
import com.datastax.hectorjpa.index.IndexOrder;
import com.datastax.hectorjpa.query.FieldExpression;
import com.datastax.hectorjpa.query.IndexQuery;
import com.datastax.hectorjpa.store.CassandraClassMetaData;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Class to perform all operations for secondary indexing on an instance in the
 * statemanager
 * 
 * @author Todd Nine
 * 
 */
public class IndexOperation {

  public static final String CF_NAME = "Index_Container";

  protected static byte[] HOLDER = new byte[] { 0 };

  private static final StringSerializer serializer = StringSerializer.get();

  private static final DynamicCompositeSerializer compositeSerializer = new DynamicCompositeSerializer();

  private static final BytesArraySerializer bytesSerializer = BytesArraySerializer
      .get();

  private static int MAX_SIZE = 500;

  /**
   * Field name to dynamic composite index mapping
   */
  private Map<String, Integer> fieldIndexes = new HashMap<String, Integer>();

  /**
   * Order name to dynamic composite index mapping
   */
  private Map<String, Integer> orderIndexes = new HashMap<String, Integer>();

  /**
   * the byte value for the row key of our index
   */
  private byte[] indexName;

  private QueryIndexField[] fields;

  private QueryOrderField[] orders;

  private IndexDefinition indexDefinition;

  private Serializer<Object> idSerializer;

  public IndexOperation(CassandraClassMetaData metaData,
      IndexDefinition indexDef) {
    this.indexDefinition = indexDef;

    FieldOrder[] fieldDirections = indexDef.getIndexedFields();
    IndexOrder[] indexOrders = indexDef.getOrderFields();

    this.fields = new QueryIndexField[fieldDirections.length];
    this.orders = new QueryOrderField[indexOrders.length];

    ByteBufferOutputStream outputStream = new ByteBufferOutputStream();

    FieldMetaData fmd = null;

    for (int i = 0; i < fieldDirections.length; i++) {
      fmd = metaData.getField(fieldDirections[i].getName());

      if (fmd == null) {
        throw new MetaDataException(
            String
                .format(
                    "You specified field '%s' as an index field, yet it does not exist in class '%s'",
                    fieldDirections[i], metaData.getDescribedType()));
      }

      fields[i] = new QueryIndexField(fmd);

      fieldIndexes.put(fmd.getName(), i);

      outputStream.write(serializer.toByteBuffer(fieldDirections[i].getName()));

    }

    for (int i = 0; i < indexOrders.length; i++) {
      fmd = metaData.getField(indexOrders[i].getName());

      if (fmd == null) {
        throw new MetaDataException(
            String
                .format(
                    "You specified field '%s' as an order field, yet it does not exist in class '%s'",
                    indexOrders[i].getName(), metaData.getDescribedType()));
      }

      orders[i] = new QueryOrderField(indexOrders[i], fmd);

      orderIndexes.put(fmd.getName(), i);

      outputStream.write(serializer.toByteBuffer(indexOrders[i].getName()));
    }

    ByteBuffer result = outputStream.getByteBuffer();

    indexName = new byte[result.limit() - result.position()];

    result.get(indexName);

    // now get our is serializer
    this.idSerializer = MappingUtils.getSerializerForPk(metaData);

    // if the ID doesn't implement comparable, we can't compare our results
    if (!Comparable.class.isAssignableFrom(metaData.getPrimaryKeyFields()[0]
        .getDeclaredType())) {
      throw new MetaDataException(
          String
              .format(
                  "Ids for indexes objects must implement Comparable.  Field '%s' on class '%s' does not implement comparable",
                  metaData.getPrimaryKeyFields()[0].getName(),
                  metaData.getDescribedType()));
    }

  }

  /**
   * Write the index definition
   * 
   * @param stateManager
   *          The objects state manager
   * @param mutator
   *          The mutator to write to
   * @param clock
   *          the clock value to use
   */
  public void writeIndex(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock) {

    DynamicComposite newComposite = null;
    DynamicComposite oldComposite = null;

    // loop through all added objects and create the writes for them.

    // create our composite of the format of id+order*
    newComposite = new DynamicComposite();

    // create our composite of the format order*+id
    oldComposite = new DynamicComposite();

    boolean changed = false;

    Object field;

    // TODO TN make this work with subclasses

    // now construct the composite with order by the ids at the end.
    for (QueryIndexField indexField : fields) {
      field = indexField.getValue(stateManager,
          stateManager.getPersistenceCapable());

      // add this to all deletes for the order composite.
      indexField.addFieldWrite(newComposite, field);

      // The deletes to teh is composite
      changed |= indexField.addFieldDelete(oldComposite, field);
    }

    // now construct the composite with order by the ids at the end.
    for (AbstractIndexField order : orders) {

      // get the field value from our current index
      field = order
          .getValue(stateManager, stateManager.getPersistenceCapable());

      // add this to all deletes for the order composite.
      order.addFieldWrite(newComposite, field);

      // The deletes to teh is composite
      changed |= order.addFieldDelete(oldComposite, field);
    }

    // add it to our new value
    newComposite.add(MappingUtils.getTargetObject(stateManager.getObjectId()),
        idSerializer);

    mutator.addInsertion(indexName, CF_NAME,
        new HColumnImpl<DynamicComposite, byte[]>(newComposite, HOLDER, clock,
            compositeSerializer, bytesSerializer));

    // value has changed since we loaded. Remove the old value
    if (changed) {

      // add it to our old value
      oldComposite.add(
          MappingUtils.getTargetObject(stateManager.getObjectId()),
          idSerializer);

      mutator.addDeletion(indexName, CF_NAME, oldComposite,
          compositeSerializer, clock);

    }
  }

  /**
   * Scan the given index query and add the results to the provided set. The set
   * comparator of the dynamic columns are compared via a tree comparator
   * 
   * @param query
   */
  public void scanIndex(IndexQuery query, Set<DynamicComposite> results,
      Keyspace keyspace) {

    DynamicComposite startScan = new DynamicComposite();
    DynamicComposite endScan = new DynamicComposite();

    int index = 0;

    // add all fields
    for (FieldExpression exp : query.getExpressions()) {
      index = this.fieldIndexes.get(exp.getField().getName());
      this.fields[index].addToComposite(startScan, index, fields.length,
          exp.getStartSliceQuery());
      this.fields[index].addToComposite(endScan, index, fields.length,
          exp.getEndSliceQuery());
    }

    // now query the values
    // get our slice range

    SliceQuery<byte[], DynamicComposite, byte[]> sliceQuery = new ThriftSliceQuery(
        keyspace, BytesArraySerializer.get(), compositeSerializer,
        BytesArraySerializer.get());

    DynamicComposite start = startScan;
    QueryResult<ColumnSlice<DynamicComposite, byte[]>> result = null;

    do {

      sliceQuery.setRange(startScan, endScan, false, MAX_SIZE);
      sliceQuery.setKey(indexName);
      sliceQuery.setColumnFamily(CF_NAME);

      result = sliceQuery.execute();

      for (HColumn<DynamicComposite, byte[]> col : result.get().getColumns()) {
        start = col.getName();
        results.add(start);
      }

    } while (result.get().getColumns().size() == MAX_SIZE);

  }

  public Comparator<DynamicComposite> getComprator() {
    return new Comparator<DynamicComposite>() {

      @Override
      public int compare(DynamicComposite c1, DynamicComposite c2) {

        //order by the size of the components - the number of orders - 1 for id as last field
        int c1StartIndex = c1.getComponents().size()-orders.length-1;
        int c2StartIndex = c1.getComponents().size()-orders.length-1;
        
        int compare = 0;
        int i = 0;

        for (; i < fields.length; i++) {

          compare = orders[i].compare(c1, c1StartIndex+i, c2, c2StartIndex+i);

          if (compare != 0) {
            return compare;
          }
        }

        // if we get here the compare fields are equal. Compare ids

        Comparable<Object> c1Id = (Comparable<Object>) c1.get(c1StartIndex+i, idSerializer);

        Comparable<Object> c2Id = (Comparable<Object>) c2.get(c2StartIndex+i, idSerializer);

        return c1Id.compareTo(c2Id);
      }

    };
  }

  /**
   * @return the indexDefinition
   */
  public IndexDefinition getIndexDefinition() {
    return indexDefinition;
  }

}
