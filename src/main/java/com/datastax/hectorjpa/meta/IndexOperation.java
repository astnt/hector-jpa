/**
 * 
 */
package com.datastax.hectorjpa.meta;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.utils.ByteBufferOutputStream;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.MetaDataException;

import com.datastax.hectorjpa.index.IndexDefinition;
import com.datastax.hectorjpa.index.IndexOrder;
import com.datastax.hectorjpa.store.CassandraClassMetaData;

/**
 * Class to perform all operations for secondary indexing on an instance
 * in the statemanager
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

  /**
   * the byte value for the row key of our index
   */
  private byte[] indexName;
  
  private QueryIndexField[] fields;

  private QueryOrderField[] orders;
  
  private IndexDefinition indexDefinition;

  public IndexOperation(CassandraClassMetaData metaData, IndexDefinition indexDef) {
    this.indexDefinition = indexDef;
    
    String[] fieldNames = indexDef.getIndexedFields();
    IndexOrder[] indexOrders = indexDef.getOrderFields();
    
    this.fields = new QueryIndexField[fieldNames.length];
    this.orders = new QueryOrderField[indexOrders.length];

    ByteBufferOutputStream outputStream = new ByteBufferOutputStream();
    
    FieldMetaData fmd = null;
  
    for (int i = 0; i < fieldNames.length; i++) {
      fmd = metaData.getField(fieldNames[i]);
      
      if(fmd == null){
        throw new MetaDataException(String.format("You specified field '%s' as an index field, yet it does not exist in class '%s'", fieldNames[i], metaData.getDescribedType()));
      }
      
      fields[i] = new QueryIndexField(fmd);
      outputStream.write(serializer.toByteBuffer(fieldNames[i]));
    }

    
    for (int i = 0; i < indexOrders.length; i++) {
      fmd = metaData.getField(indexOrders[i].getName());
      
      if(fmd == null){
        throw new MetaDataException(String.format("You specified field '%s' as an order field, yet it does not exist in class '%s'", indexOrders[i].getName(), metaData.getDescribedType()));
      }
      
      
      orders[i] = new QueryOrderField(indexOrders[i], fmd);
      outputStream.write(serializer.toByteBuffer(indexOrders[i].getName()));
    }

    ByteBuffer result = outputStream.getByteBuffer();

    indexName = new byte[result.limit() - result.position()];

    result.get(indexName);

  }

  /**
   * Write the index definition
   * @param stateManager The objects state manager
   * @param mutator The mutator to write to
   * @param clock the clock value to use
   */
  public void writeIndex(OpenJPAStateManager stateManager, Mutator<byte[]> mutator, long clock) {

    DynamicComposite newComposite = null;
    DynamicComposite oldComposite = null;

    // loop through all added objects and create the writes for them.

    // create our composite of the format of id+order*
    newComposite = new DynamicComposite();

    // create our composite of the format order*+id
    oldComposite = new DynamicComposite();

    boolean changed = false;

    Object field;
    
    //TODO TN make this work with subclasses
    
    // now construct the composite with order by the ids at the end.
    for (QueryIndexField indexField : fields) {
      field = indexField.getValue(stateManager, stateManager.getPersistenceCapable());
      
      // add this to all deletes for the order composite.
      indexField.addFieldWrite(newComposite, field);

      // The deletes to teh is composite
      changed |= indexField.addFieldDelete(oldComposite, field);
    }
    

    // now construct the composite with order by the ids at the end.
    for (AbstractIndexField order : orders) {

      // get the field value from our current index
      field = order .getValue(stateManager, stateManager.getPersistenceCapable());
      
      // add this to all deletes for the order composite.
      order.addFieldWrite(newComposite, field);

      // The deletes to teh is composite
      changed |= order.addFieldDelete(oldComposite, field);
    }

    mutator.addInsertion(indexName, CF_NAME,
        new HColumnImpl<DynamicComposite, byte[]>(newComposite, HOLDER, clock,
            compositeSerializer, bytesSerializer));

    // value has changed since we loaded. Remove the old value
    if (changed) {
      mutator.addDeletion(indexName, CF_NAME, oldComposite,
          compositeSerializer, clock);

    }
  }

  /**
   * @return the indexDefinition
   */
  public IndexDefinition getIndexDefinition() {
    return indexDefinition;
  }
}
