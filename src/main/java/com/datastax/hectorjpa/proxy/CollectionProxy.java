/**
 * 
 */
package com.datastax.hectorjpa.proxy;

import java.util.Iterator;

import org.apache.openjpa.util.AbstractLRSProxyCollection;

import com.datastax.hectorjpa.meta.CollectionField;

/**
 * The proxy for large collections.  Need to use the Collection field to better fetch data and page
 * 
 * TODO TN finish this and build a better interface into collectionfield for paging
 * 
 * @author Todd Nine
 *
 */
public class CollectionProxy extends AbstractLRSProxyCollection {

  /**
   * The backing collection field to perform all I/O for us
   */
  private CollectionField field;
  
  //TODO TN Update this to return real values
  
  public CollectionProxy(Class elementType, boolean ordered, CollectionField field) {
    super(elementType, ordered);
    
    this.field = field;

  }

  /* (non-Javadoc)
   * @see org.apache.openjpa.util.AbstractLRSProxyCollection#itr()
   */
  @Override
  protected Iterator itr() {
   return new CollectionIterator(field);
  }

  /* (non-Javadoc)
   * @see org.apache.openjpa.util.AbstractLRSProxyCollection#has(java.lang.Object)
   */
  @Override
  protected boolean has(Object o) {
    // TODO Auto-generated method stub
    return false;
  }

  /* (non-Javadoc)
   * @see org.apache.openjpa.util.AbstractLRSProxyCollection#count()
   */
  @Override
  protected int count() {
    // TODO Auto-generated method stub
    return 0;
  }

  
  private class CollectionIterator implements Iterator{

    private CollectionField field;
    
    public CollectionIterator(CollectionField field){
      this.field = field;
      
    
    }
    
    @Override
    public boolean hasNext() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Object next() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void remove() {
      // TODO Auto-generated method stub
      
    }
    
  }
  
}
