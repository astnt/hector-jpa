package com.datastax.hectorjpa.serializer;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;

/**
 * Uses IntSerializer via translating Doubles to and from raw long bytes form.
 * 
 * @author Todd Nine
 */
public class FloatSerializer extends AbstractSerializer<Float> {

  private static final FloatSerializer instance = new FloatSerializer();

  public static FloatSerializer get() {
    return instance;
  }
  
  @Override
  public ByteBuffer toByteBuffer(Float obj) {
    return IntegerSerializer.get().toByteBuffer(Float.floatToRawIntBits(obj));
  }

  @Override
  public Float fromByteBuffer(ByteBuffer bytes) {
    return Float.intBitsToFloat(IntegerSerializer.get().fromByteBuffer(bytes));
  }

}

