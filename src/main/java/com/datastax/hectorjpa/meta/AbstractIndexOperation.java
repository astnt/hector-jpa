/**
 * 
 */
package com.datastax.hectorjpa.meta;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.MetaDataException;

import com.datastax.hectorjpa.index.FieldOrder;
import com.datastax.hectorjpa.index.IndexDefinition;
import com.datastax.hectorjpa.index.IndexOrder;
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
public abstract class AbstractIndexOperation {

  public static final String CF_NAME = "Index_Container";

  protected static byte[] HOLDER = new byte[] { 0 };

  protected static final StringSerializer stringSerializer = StringSerializer
      .get();

  protected static final DynamicCompositeSerializer compositeSerializer = new DynamicCompositeSerializer();

  protected static final BytesArraySerializer bytesSerializer = BytesArraySerializer
      .get();

  protected static int MAX_SIZE = 500;

  /**
   * Field name to dynamic composite index mapping
   */
  protected Map<String, Integer> fieldIndexes = new HashMap<String, Integer>();

  /**
   * the byte value for the row key of our index
   */
  protected byte[] indexName;

  protected QueryIndexField[] fields;

  protected QueryOrderField[] orders;

  protected IndexDefinition indexDefinition;

  protected Serializer<Object> idSerializer;

  public AbstractIndexOperation(CassandraClassMetaData metaData,
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
                    fieldDirections[i].getName(), metaData.getDescribedType()));
      }

      fields[i] = new QueryIndexField(fmd);

      fieldIndexes.put(fmd.getName(), i);

      outputStream.write(stringSerializer.toByteBuffer(fieldDirections[i]
          .getName()));

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

      outputStream
          .write(stringSerializer.toByteBuffer(indexOrders[i].getName()));
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
   * Write the index
   * 
   * @param stateManager
   * @param mutator
   * @param clock
   */
  public abstract void writeIndex(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock);

  /**
   * Scan the given index query and add the results to the provided set. The set
   * comparator of the dynamic columns are compared via a tree comparator
   * 
   * @param query
   */
  public abstract void scanIndex(IndexQuery query,
      Set<DynamicComposite> results, Keyspace keyspace);

  /**
   * Construct the 2 composites from the fields in this index. Returns true if
   * the index values have changed.
   * 
   * @param newComposite
   * @param oldComposite
   * @return
   */
  protected boolean constructComposites(DynamicComposite newComposite,
      DynamicComposite oldComposite, OpenJPAStateManager stateManager) {

    boolean changed = false;

    Object key = MappingUtils.getTargetObject(stateManager.getObjectId());

    Object field;

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

    newComposite.add(key, idSerializer);

    oldComposite.add(key, idSerializer);

    return changed;
  }

  /**
   * Execute a query with the given start and end dynamic composites
   * 
   * @param start
   *          The start value from the range scan
   * @param end
   *          The end value in the range scan
   * @param results
   *          The results to add the returned values to. Sorted by order fields,
   *          then id
   * @param keyspace
   *          The kesypace
   */
  protected void executeQuery(DynamicComposite start, DynamicComposite end,
      Set<DynamicComposite> results, Keyspace keyspace) {

    SliceQuery<byte[], DynamicComposite, byte[]> sliceQuery = new ThriftSliceQuery(
        keyspace, BytesArraySerializer.get(), compositeSerializer,
        BytesArraySerializer.get());

    DynamicComposite startScan = start;
    QueryResult<ColumnSlice<DynamicComposite, byte[]>> result = null;

    do {

      sliceQuery.setRange(startScan, end, false, MAX_SIZE);
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

        // order by the size of the components - the number of orders 
        int c1StartIndex = c1.getComponents().size() - orders.length-1;
        int c2StartIndex = c1.getComponents().size() - orders.length-1;

        int compare = 0;
        int i = 0;

        for (; i < orders.length; i++) {

          compare = orders[i].compare(c1, c1StartIndex + i, c2, c2StartIndex
              + i);

          if (compare != 0) {
            return compare;
          }
        }

        // if we get here the compare fields are equal. Compare ids

        Comparable<Object> c1Id = (Comparable<Object>) c1.get(c1StartIndex + i,
            idSerializer);

        Comparable<Object> c2Id = (Comparable<Object>) c2.get(c2StartIndex + i,
            idSerializer);

        return c1Id.compareTo(c2Id);
      }

    };
  }

  /**
   * Allocate a dynamic composite to the given length
   * 
   * @param length
   * @return
   */
  protected DynamicComposite allocateComposite(int length) {

    List<Serializer<?>> serializers = new ArrayList<Serializer<?>>(length);

    for (int i = 0; i < length; i++) {
      serializers.add(null);
    }

    DynamicComposite composite = new DynamicComposite();

    composite.setSerializersByPosition(serializers);
    
    return composite;

  }

  /**
   * @return the indexDefinition
   */
  public IndexDefinition getIndexDefinition() {
    return indexDefinition;
  }

}
