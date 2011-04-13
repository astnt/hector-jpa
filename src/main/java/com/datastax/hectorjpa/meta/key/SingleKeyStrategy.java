/**
 * 
 */
package com.datastax.hectorjpa.meta.key;

import java.nio.ByteBuffer;

import me.prettyprint.hector.api.Serializer;

import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.MetaDataException;
import org.apache.openjpa.util.OpenJPAId;

import com.datastax.hectorjpa.store.CassandraClassMetaData;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * @author Todd Nine
 * 
 */
public class SingleKeyStrategy implements KeyStrategy {

  private final Serializer<Object> idSerializer;

  public SingleKeyStrategy(CassandraClassMetaData classMetaData) {
    FieldMetaData[] fields = classMetaData.getPrimaryKeyFields();

    if (fields.length > 1) {
      throw new MetaDataException(
          "Single field strategy cannot have more than 1 primary key field");
    }

    idSerializer = MappingUtils.getSerializer(fields[0]);
  }

 
  @Override
  public ByteBuffer toByteBuffer(Object oid) {
    Object id = ((OpenJPAId)oid).getIdObject();
    
    return idSerializer.toByteBuffer(id);
  }

  @Override
  public byte[] toByteArray(Object oid) {
    Object id = ((OpenJPAId)oid).getIdObject();
    
    if(id == null){
      return null;
    }
    
    return idSerializer.toBytes(id);
  }
  
  

  @Override
  public Object getInstance(ByteBuffer bytes) {
    return idSerializer.fromByteBuffer(bytes);
  }


  @Override
  public Object getInstance(byte[] bytes) {
    return idSerializer.fromBytes(bytes);
  }

}
