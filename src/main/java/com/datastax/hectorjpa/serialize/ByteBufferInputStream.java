/**
 * 
 */
package com.datastax.hectorjpa.serialize;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Simple wrapper that wraps input streams around byte Buffers
 * 
 * @author Todd Nine
 * 
 */
public class ByteBufferInputStream extends InputStream {

  private ByteBuffer buffer;

  public ByteBufferInputStream(ByteBuffer buff) {
    this.buffer = buff;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.InputStream#read()
   */
  @Override
  public int read() throws IOException {
    if (!buffer.hasRemaining()) {
      return -1;
    }

    return buffer.get();
  }

}
