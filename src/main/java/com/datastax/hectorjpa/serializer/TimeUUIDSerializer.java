/**
 * 
 */
package com.datastax.hectorjpa.serializer;

import java.nio.ByteBuffer;

import com.eaio.uuid.UUID;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;

/**
 * Serializer for the time UUID  object
 * 
 * @author Todd Nine
 *
 */
public class TimeUUIDSerializer extends AbstractSerializer<UUID>{


    private static final TimeUUIDSerializer instance = new TimeUUIDSerializer();

    public static TimeUUIDSerializer get() {
      return instance;
    }

    public ByteBuffer toByteBuffer(UUID uuid) {
      if (uuid == null) {
        return null;
      }
      long msb = uuid.getTime();
      long lsb = uuid.getClockSeqAndNode();
      
      byte[] buffer = new byte[16];

      for (int i = 0; i < 8; i++) {
        buffer[i] = (byte) (msb >>> 8 * (7 - i));
      }
      for (int i = 8; i < 16; i++) {
        buffer[i] = (byte) (lsb >>> 8 * (7 - i));
      }

      return ByteBuffer.wrap(buffer);
    }

    public UUID fromByteBuffer(ByteBuffer bytes) {
      if (bytes == null) {
        return null;
      }    
      return new UUID(bytes.getLong(), bytes.getLong());
    }

  
}
