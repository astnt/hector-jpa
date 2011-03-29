package com.datastax.hectorjpa.meta;

import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.openjpa.meta.ClassMetaData;

import com.datastax.hectorjpa.index.FieldOrder;
import com.datastax.hectorjpa.index.IndexDefinition;
import com.datastax.hectorjpa.index.IndexOrder;
import com.datastax.hectorjpa.store.CassandraClassMetaData;
import com.datastax.hectorjpa.store.EntityFacade;

/**
 * Cache for holding all meta data
 * 
 * @author Todd Nine
 * 
 */
public class MetaCache {

  private final ConcurrentMap<ClassMetaData, EntityFacade> metaData = new ConcurrentHashMap<ClassMetaData, EntityFacade>();

  private final ConcurrentMap<String, CassandraClassMetaData> discriminators = new ConcurrentHashMap<String, CassandraClassMetaData>();

  private final SortedMap<IndexDefinition, IndexOperation> indexDefinitions = new TreeMap<IndexDefinition, IndexOperation>(new IndexOpComparator());

  /**
   * Create a new meta cache for classes
   * 
   */
  public MetaCache() {
   
  }

  /**
   * Get the entity facade for this class. If it does not exist it is created
   * and added to the cache
   * 
   * @param meta
   * @return
   */
  public EntityFacade getFacade(ClassMetaData meta) {

    CassandraClassMetaData cassMeta = (CassandraClassMetaData) meta;

    EntityFacade facade = metaData.get(cassMeta);

    if (facade != null) {
      return facade;
    }

    facade = new EntityFacade(cassMeta);

    metaData.putIfAbsent(cassMeta, facade);

    String discriminatorValue = cassMeta.getDiscriminatorColumn();

    if (discriminatorValue != null) {

      discriminators.putIfAbsent(discriminatorValue, cassMeta);
    }
    
    IndexOperation[] indexOps = facade.getIndexOps();
    
    if(indexOps != null){
      for(IndexOperation op: indexOps){
        indexDefinitions.put(op.getIndexDefinition(), op);
      }
      
    }

    return facade;

  }

  /**
   * Get the class name from the discriminator string. Null if one doesn't exist
   * 
   * @param discriminator
   * @return
   */
  public CassandraClassMetaData getClassFromDiscriminator(String discriminator) {
    return discriminators.get(discriminator);
  }

  /**
   * Return an index that will match all the given fields and has the current
   * orders. If one does not exist null will be returned.
   * 
   * @param cmd
   * @param fields
   * @param orders
   * @return
   */
  public IndexOperation getIndexOperation(CassandraClassMetaData cmd,
      FieldOrder[] fields, IndexOrder[] orders) {

    IndexDefinition temp = new IndexDefinition(cmd, fields, orders);

    return indexDefinitions.get(temp);

  }

  /**
   * Return an index that can be used for searching the fields provided. The
   * order must be an exact match, however there may be more fields in the index
   * provided arguments.
   * 
   * @param cmd
   * @param fields
   * @param orders
   * @return
   */
  public IndexOperation getValidIndex(CassandraClassMetaData cmd,
      String[] fields, IndexOrder[] orders) {

    /**
     * TODO this
     */
    return null;

  }

  /**
   * Compare 2 index definitions. Index definitions are compared in the
   * following way
   * 
   * If an order is defined, then all order fields must be preset and in the
   * same order for order comparison to == 0 If these orders are not the same,
   * the shortest number of order operands is returned as less.
   * 
   * If the order operands are the same, the fields are compared. The fields
   * follow the same logic of operands Indexes with less fields are returned
   * with < 0 to encourage the use of shorter rows for faster querying
   * 
   * Not a null safe comparator
   * 
   * @author Todd Nine
   * 
   */
  private class IndexOpComparator implements Comparator<IndexDefinition> {

    @Override
    public int compare(IndexDefinition def1, IndexDefinition def2) {

      int compare = def1.getMetaData().compareTo(def2.getMetaData());

      if (compare != 0) {
        return compare;
      }

      IndexOrder[] def1Order = def1.getOrderFields();

      IndexOrder[] def2Order = def2.getOrderFields();

      if (def1Order.length > def2Order.length) {
        return 1;
      } else if (def1Order.length < def2Order.length) {
        return -1;
      }

      // fields are same length, compare them
      for (int i = 0; i < def1Order.length; i++) {
        compare = def1Order[i].getName().compareTo(def2Order[i].getName());

        if (compare != 0) {
          return compare;
        }

      }

      // our orders matched, now compare fields
      FieldOrder[] def1Field = def1.getIndexedFields();
      FieldOrder[] def2Field = def2.getIndexedFields();

      if (def1Field.length > def2Field.length) {
        return 1;
      } else if (def1Field.length < def2Field.length) {
        return -1;
      }

      // lengths are the same compare the fields
      for (int i = 0; i < def1Order.length; i++) {
        compare = def1Field[i].compareTo(def2Field[i]);

        if (compare != 0) {
          return compare;
        }

      }

      // same
      return 0;

    }

  }
  

}
