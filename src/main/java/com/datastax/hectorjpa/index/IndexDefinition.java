/**
 * 
 */
package com.datastax.hectorjpa.index;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.utils.ByteBufferOutputStream;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.FieldMetaData;

import com.datastax.hectorjpa.meta.AbstractOrderField;
import com.datastax.hectorjpa.meta.QueryOrderField;

/**
 * @author Todd Nine
 * 
 */
public class IndexDefinition {

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

  private AbstractOrderField[] orders;

  public IndexDefinition(FieldMetaData fmd, IndexOrder[] indexOrders) {
    this.orders = new AbstractOrderField[indexOrders.length];

    ByteBufferOutputStream outputStream = new ByteBufferOutputStream();
    outputStream.write(serializer.toByteBuffer(fmd.getName()));

    for (int i = 0; i < indexOrders.length; i++) {
      orders[i] = new QueryOrderField(indexOrders[i], fmd);
      outputStream.write(serializer.toByteBuffer(indexOrders[i].getName()));
    }

    ByteBuffer result = outputStream.getByteBuffer();

    indexName = new byte[result.limit() - result.position()];

    result.get(indexName);

  }

  /**
   * Write the index definition
   * @param stateManager
   * @param fieldValue
   * @param serializer
   * @param mutator
   * @param clock
   */
  public void writeIndex(OpenJPAStateManager stateManager, Object fieldValue,
      Serializer<?> serializer, Mutator<byte[]> mutator, long clock) {

    DynamicComposite newComposite = null;
    DynamicComposite oldComposite = null;

    // loop through all added objects and create the writes for them.

    // create our composite of the format of id+order*
    newComposite = new DynamicComposite();

    // create our composite of the format order*+id
    oldComposite = new DynamicComposite();

    boolean changed = false;

    Object field;

    // now construct the composite with order by the ids at the end.
    for (AbstractOrderField order : orders) {

      // get the field value from our current index
      field = order
          .getValue(stateManager, stateManager.getPersistenceCapable());

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

}
