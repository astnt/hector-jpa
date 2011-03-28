package com.datastax.hectorjpa.query;

import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.lib.rop.ResultObjectProviderIterator;

/**
 * 
 * @author Todd Nine
 *
 */
public class CassandraResultObjectProvider implements ResultObjectProvider {

  @Override
  public boolean supportsRandomAccess() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void open() throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Object getResultObject() throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean next() throws Exception {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean absolute(int pos) throws Exception {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int size() throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void reset() throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void handleCheckedException(Exception e) {
    // TODO Auto-generated method stub
    
  }

}
