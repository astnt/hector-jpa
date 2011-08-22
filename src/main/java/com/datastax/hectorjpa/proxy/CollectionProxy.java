/**
 * 
 */
package com.datastax.hectorjpa.proxy;

import java.util.Iterator;

import org.apache.openjpa.util.AbstractLRSProxyCollection;

import com.datastax.hectorjpa.meta.collection.OrderedCollectionField;

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
  private OrderedCollectionField field;
  
  //TODO TN Update this to return real values
  
  public CollectionProxy(Class elementType, boolean ordered, OrderedCollectionField field) {
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
    return false;
  }

  /* (non-Javadoc)
   * @see org.apache.openjpa.util.AbstractLRSProxyCollection#count()
   */
  @Override
  protected int count() {
    return 0;
  }

  
  private class CollectionIterator implements Iterator{

    private OrderedCollectionField field;
    
    public CollectionIterator(OrderedCollectionField field){
      this.field = field;
      
    
    }
    
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public Object next() {
      return null;
    }

    @Override
    public void remove() {
      
    }
    
  }
  
}
