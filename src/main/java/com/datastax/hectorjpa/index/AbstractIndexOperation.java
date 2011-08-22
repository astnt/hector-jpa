/**
 * 
 */
package com.datastax.hectorjpa.index;

import static com.datastax.hectorjpa.serializer.CompositeUtils.getCassType;
import static com.datastax.hectorjpa.serializer.CompositeUtils.newComposite;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.utils.ByteBufferOutputStream;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.MetaDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.meta.key.KeyStrategy;
import com.datastax.hectorjpa.query.IndexQuery;
import com.datastax.hectorjpa.query.QueryIndexField;
import com.datastax.hectorjpa.query.QueryOrderField;
import com.datastax.hectorjpa.query.iterator.ScanBuffer;
import com.datastax.hectorjpa.service.IndexAudit;
import com.datastax.hectorjpa.service.IndexQueue;
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
  

  private static final Logger logger = LoggerFactory.getLogger(AbstractIndexOperation.class);
 

  public static final String CF_NAME = "Index_Container";
  
  /**
   * The version to prepend to every row key for indexing
   */
  public static final int INDEXING_VERSION = 2;

  protected static byte[] HOLDER = new byte[] { 0 };

  protected static final StringSerializer stringSerializer = StringSerializer
      .get();

  protected static final DynamicCompositeSerializer compositeSerializer = new DynamicCompositeSerializer();

  protected static final BytesArraySerializer bytesSerializer = BytesArraySerializer
      .get();
  
  protected static final ByteBufferSerializer buffSerializer = ByteBufferSerializer.get();

  protected static int MAX_SIZE = 500;

  /**
   * the byte value for the row key of our index with the version prepended to the index name
   */
  protected byte[] indexName;

  /**
   * The index name as a string.  Not really used during runtime other than for debugging output
   */
  protected String searchIndexNameString;
  
  protected String reverseIndexNameString;

  /**
   * The bytes that represent the reverse index
   */
  protected byte[] reverseIndexName;

  protected QueryIndexField[] fields;

  protected QueryOrderField[] orders;

  protected IndexDefinition indexDefinition;

  protected KeyStrategy keyStrategy;
  
  protected String compositeComparator;

  public AbstractIndexOperation(CassandraClassMetaData metaData,
      IndexDefinition indexDef) {
    this.indexDefinition = indexDef;

    FieldOrder[] fieldDirections = indexDef.getIndexedFields();
    IndexOrder[] indexOrders = indexDef.getOrderFields();

    this.fields = new QueryIndexField[fieldDirections.length];
    this.orders = new QueryOrderField[indexOrders.length];

    StringBuffer searchIndexName = new StringBuffer();
    StringBuffer reverseIndexName = new StringBuffer();
   
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

      searchIndexName.append(fieldDirections[i].getName()).append("_");
      reverseIndexName.append(fieldDirections[i].getName()).append("_");
      

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

      String order = indexOrders[i].isAscending() ? "Asc" : "Desc";
      
      searchIndexName.append(indexOrders[i].getName()).append(order).append("_");
      reverseIndexName.append(indexOrders[i].getName()).append(order).append("_");
    }

    searchIndexName.append("search");
    reverseIndexName.append("reverse");
    
    
    searchIndexNameString = searchIndexName.toString();
    reverseIndexNameString = reverseIndexName.toString();
    
    ByteBuffer result = ByteBuffer.allocate(searchIndexNameString.length()+4);
    result.putInt(INDEXING_VERSION);
    result.put(StringSerializer.get().toBytes(searchIndexNameString));
    result.rewind();

    indexName = new byte[result.limit() - result.position()];

    result.get(this.indexName);

    result = ByteBuffer.allocate(reverseIndexNameString.length()+4);
    result.putInt(INDEXING_VERSION);
    result.put(StringSerializer.get().toBytes(reverseIndexNameString));
    result.rewind();

    this.reverseIndexName = new byte[result.limit() - result.position()];

    result.get(this.reverseIndexName);

    // now get our is serializer
    keyStrategy = MappingUtils.getKeyStrategy(metaData);

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
    
    
    ByteBufferOutputStream searchIndexNameBuff = new ByteBufferOutputStream();
    ByteBufferOutputStream reverseIndexNameBuff = new ByteBufferOutputStream();

    
    searchIndexNameBuff.write(IntegerSerializer.get().toByteBuffer(INDEXING_VERSION));
    reverseIndexNameBuff.write(IntegerSerializer.get().toByteBuffer(INDEXING_VERSION));
    
    
    compositeComparator = getCassType(buffSerializer);

  }

  /**
   * Write the index
   * 
   * @param stateManager
   * @param mutator
   * @param clock
   */
  public abstract void writeIndex(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, IndexQueue queue);

  /**
   * Prepare and return the singlescaniterator for the result set
   * 
   * @param query
   */
  public abstract ScanBuffer scanIndex(IndexQuery query, Keyspace keyspace);

  /**
   * Remove all values from the index that were for the given statemanager
   * 
   * @param stateManager
   * @param queue
   */
  public void removeIndexes(OpenJPAStateManager stateManager, IndexQueue queue,
      long clock) {

    ByteBuffer key = keyStrategy.toByteBuffer(stateManager.fetchObjectId());

    DynamicComposite composite = newComposite();

    composite.addComponent(key, buffSerializer);

    // queue the index values to be deleted
    queue.addDelete(new IndexAudit(indexName, reverseIndexName, composite,
        clock, CF_NAME, true));

  }

  /**
   * Construct the 2 composites from the fields in this index. Returns true if
   * the index values have changed.
   * 
   * @param searchComposite
   * @param oldComposite
   * @return
   */
  protected void constructComposites(DynamicComposite searchComposite, DynamicComposite tombstoneComposite,
      DynamicComposite auditComposite, OpenJPAStateManager stateManager) {

    ByteBuffer key = keyStrategy.toByteBuffer(stateManager.fetchObjectId());

    Object field;

    tombstoneComposite.setComponent(0, key, buffSerializer, compositeComparator, ComponentEquality.EQUAL);

    auditComposite.setComponent(0, key, buffSerializer, 	compositeComparator, ComponentEquality.EQUAL);

    // now construct the composite with order by the ids at the end.
    for (QueryIndexField indexField : fields) {
      field = indexField.getValue(stateManager,
          stateManager.getPersistenceCapable());

      // add this to all deletes for the order composite.
      indexField.addFieldWrite(searchComposite, field);
      indexField.addFieldWrite(tombstoneComposite, field);

      
    }

    // now construct the composite with order by the ids at the end.
    for (AbstractIndexField order : orders) {

      // get the field value from our current index
      field = order
          .getValue(stateManager, stateManager.getPersistenceCapable());

      // add this to all deletes for the order composite.
      order.addFieldWrite(searchComposite, field);
      order.addFieldWrite(tombstoneComposite, field);

    }

    // add it to our new value

    searchComposite.addComponent(key, buffSerializer, compositeComparator);

    if(logger.isDebugEnabled()){
      logger.debug("Writing search index with row key: {} and column: {}", this.searchIndexNameString, ByteBufferUtil.bytesToHex(searchComposite.serialize()));
      logger.debug("Writing tombstone index with row key: {} and column: {}", this.reverseIndexNameString, ByteBufferUtil.bytesToHex(tombstoneComposite.serialize()));
    }
    

  }


  public Comparator<DynamicComposite> getComprator() {
    return new ResultComparator();
  }

  /**
   * @return the indexDefinition
   */
  public IndexDefinition getIndexDefinition() {
    return indexDefinition;
  }

  public class ResultComparator implements Comparator<DynamicComposite> {

    @SuppressWarnings("unchecked")
    @Override
    public int compare(DynamicComposite c1, DynamicComposite c2) {

      if (c1 == null && c2 != null) {
        return -1;
      }

      if (c2 == null && c1 != null) {
        return 1;
      }

      if (c1 == null && c2 == null) {
        return 0;
      }

      int compare = 0;

      int size = 0;

      Comparable<Object> c1Id = null;
      Comparable<Object> c2Id = null;

      // no order by, just order by each field starting from the beginning
      if (orders.length == 0) {

        size = fields.length;

        for (int i = 0; i < size; i++) {
          c1Id = (Comparable<Object>) c1.get(i, fields[i].getSerializer());

          c2Id = (Comparable<Object>) c2.get(i, fields[i].getSerializer());

          compare = c1Id.compareTo(c2Id);

          if (compare != 0) {
            return compare;
          }
        }
        
        ByteBuffer id1 = c1.get(c1.size() -1, buffSerializer);

        c1Id = (Comparable<Object>) keyStrategy.getInstance(id1);
        
        ByteBuffer id2 = c2.get(c2.size() -1, buffSerializer);

        c2Id = (Comparable<Object>) keyStrategy.getInstance(id2);

        return c1Id.compareTo(c2Id);

      }

      size = c1.getComponents().size();

      int c1StartIndex = size - orders.length - 1; // c1.getComponents().size()
      // -
      // orders.length-2;
      int c2StartIndex = size - orders.length - 1; // c1.getComponents().size()
      // -
      // orders.length-2;

      for (int i = 0; i < orders.length; i++) {

        compare = orders[i].compare(c1, c1StartIndex + i, c2, c2StartIndex + i);

        if (compare != 0) {
          return compare;
        }
      }

      // if we get here the compare fields are equal. Compare ids
      ByteBuffer id1 = c1.get(c1.size() -1, buffSerializer);

      c1Id = (Comparable<Object>) keyStrategy.getInstance(id1);
      
      ByteBuffer id2 = c2.get(c2.size() -1, buffSerializer);

      c2Id = (Comparable<Object>) keyStrategy.getInstance(id2);
      

      return c1Id.compareTo(c2Id);

    }

  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("fields=")
    .append(Arrays.asList(fields))
    .append(",indexDefinition=")
    .append(indexDefinition)
    .append(",indexName=")
    .append(indexName)
    .append(",keyStrategy=")
    .append(keyStrategy)
    .append(",orders=")
    .append(Arrays.asList(orders))
    .append(",reverseIndexName=")
    .append(reverseIndexName);
    return sb.toString();
  }
  
  

}
