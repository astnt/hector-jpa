package com.datastax.hectorjpa.serializer;

import static me.prettyprint.hector.api.ddl.ComparatorType.BYTESTYPE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

public class BigDecimalSerializer extends AbstractSerializer<BigDecimal> {

  private static final BigDecimalSerializer INSTANCE = new BigDecimalSerializer();

  public static BigDecimalSerializer get() {
    return INSTANCE;
  }

  @Override
  public BigDecimal fromByteBuffer(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }

    int scale = byteBuffer.getInt();

    int length = byteBuffer.remaining();
    byte[] bytes = new byte[length];
    byteBuffer.duplicate().get(bytes);

    return new BigDecimal(new BigInteger(bytes), scale);
  }

  @Override
  public ByteBuffer toByteBuffer(BigDecimal obj) {
    if (obj == null) {
      return null;
    }

    byte[] unscaled = obj.unscaledValue().toByteArray();

    ByteBuffer buff = ByteBuffer.allocate(unscaled.length + 4);

    buff.putInt(obj.scale());
    buff.put(unscaled);

    return buff;
  }

  @Override
  public ComparatorType getComparatorType() {
    return BYTESTYPE;
  }

}
