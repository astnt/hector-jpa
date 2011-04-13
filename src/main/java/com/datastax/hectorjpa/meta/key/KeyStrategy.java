/**
 * 
 */
package com.datastax.hectorjpa.meta.key;

import java.nio.ByteBuffer;

import org.apache.openjpa.kernel.OpenJPAStateManager;

/**
 * @author Todd Nine
 *
 */
public interface KeyStrategy {

 
  
  /**
   * Get the bytes that represent this key.  Can either be a single field or a multi field
   * @param target
   * @return
   */
  public ByteBuffer toByteBuffer(Object oid);
  
  /**
   * Get the value as a byte array
   * @param target
   * @return
   */
  public byte[] toByteArray(Object oid);
  
  /**
   * Get an instance of the Id from the given bytes.  This should be the native type, not the wrapped JPA Oid
   * @param field
   * @return
   */
  public Object getInstance(ByteBuffer bytes);
  
  /**
   * Get the instance from bytes
   * @param bytes
   * @return
   */
  public Object getInstance(byte[] bytes);

//  /**
//   * construct the byte array from the oid for the primary key
//   * @param oid
//   * @return
//   */
//  public byte[] fromOid(Object oid);
}
