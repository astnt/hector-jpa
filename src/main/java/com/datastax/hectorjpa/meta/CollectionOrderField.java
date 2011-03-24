package com.datastax.hectorjpa.meta;

import java.util.Iterator;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.Order;
import org.apache.openjpa.util.Proxy;
import org.apache.openjpa.util.UserException;

import com.datastax.hectorjpa.proxy.ProxyUtils;
import com.datastax.hectorjpa.store.MappingUtils;

/**
 * Inner class to encapsulate order field logic and meta data
 * 
 * @author Todd Nine
 * 
 */
public class CollectionOrderField extends AbstractOrderField{


  public CollectionOrderField(Order order, FieldMetaData fmd) {
    super(order, fmd);
  }

  @Override
  protected ClassMetaData getContainerClassMetaData(FieldMetaData fmd) {
    return fmd.getElement().getTypeMetaData();
  }

 
 
}