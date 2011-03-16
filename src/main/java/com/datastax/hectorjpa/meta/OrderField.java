package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.Serializer;

import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;
import org.apache.openjpa.util.MetaDataException;

import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Class to represent order from fields on relationships. Holds the order
 * clause, the index field name and the pointer to the actual field to serialize
 * 
 * @author Todd Nine
 * 
 */
public class OrderField {

  private Order order;
  private Serializer<?> serializer;

  // array of fields Id's from our root reference to get the property to load
  private int[] fieldIds;

  public OrderField(Order order, FieldMetaData fmd) {
    super();
    this.order = order;
    
    //TODO, this will most likely need moved somewhere that is common access for both order and index fields
    //since both could potentially be recursive

    String name = order.getName();

    String[] props = name.split(".");

    fieldIds = new int[props.length];

    ClassMetaData current = fmd.getDeclaredTypeMetaData();

    FieldMetaData meta = null;

    for (int i = 0; i < props.length; i++) {
      meta = current.getField(props[i]);

      // user has a value we can't find the field
      if (meta == null) {
        throw new MetaDataException(
            String
                .format(
                    "Could not find the field with name '%s' on class '%s' in the order clause '%s'",
                    props[i], current.getDescribedType().getName(), name));
      }

      fieldIds[i] = meta.getIndex();

      current = meta.getDeclaredTypeMetaData();

    }

    this.serializer = MappingUtils.getSerializer(meta);

  }

}
