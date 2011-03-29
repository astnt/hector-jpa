/**
 * 
 */
package com.datastax.hectorjpa.index;

import java.util.ArrayList;
import java.util.List;

import org.apache.openjpa.meta.FieldMetaData;

import serp.util.Strings;

import com.datastax.hectorjpa.store.CassandraClassMetaData;

/**
 * A wrapper for all index definitions on a given field
 * 
 * @author Todd Nine
 * 
 */
public class IndexDefinitions {

  /**
   * External name for index definitions
   */
  public static final String EXT_NAME = IndexDefinitions.class.getName();

  private List<IndexDefinition> indexDefs;

  public IndexDefinitions() {
    indexDefs = new ArrayList<IndexDefinition>();
  }

  /**
   * Get all orders in the list
   * 
   * @return
   */
  public List<IndexDefinition> getDefinitions() {
    return this.indexDefs;
  }

  /**
   * Add a new index definition for the given field with the order expression.
   * This is mostly copied from the default annotation scanner
   * 
   * @param orderExpression
   * @param field
   */
  public void add(String fieldExpression, String orderExpression,
      CassandraClassMetaData metaData) {
    String[] decs = Strings.split(orderExpression, ",", 0);
    IndexOrder[] orders = new IndexOrder[decs.length];

    int spc;

    boolean asc;

    for (int i = 0; i < decs.length; i++) {
      decs[i] = decs[i].trim();

      spc = decs[i].indexOf(' ');
      if (spc == -1)
        asc = true;
      else {
        asc = decs[i].substring(spc + 1).trim().toLowerCase().startsWith("asc");
        decs[i] = decs[i].substring(0, spc);
      }
      orders[i] = new IndexOrder(decs[i], asc);

      // set "isUsedInOrderBy" to the field
      FieldMetaData fmd = metaData.getDeclaredField(decs[i]);
      if (fmd != null)
        fmd.setUsedInOrderBy(true);

    }

    decs = Strings.split(fieldExpression, ",", 0);
    FieldOrder[] fields = new FieldOrder[decs.length];

    for (int i = 0; i < fields.length; i++) {
      decs[i] = decs[i].trim();

      spc = decs[i].indexOf(' ');
      if (spc == -1)
        asc = true;
      else {
        asc = decs[i].substring(spc + 1).trim().toLowerCase().startsWith("asc");
        decs[i] = decs[i].substring(0, spc);
      }
      fields[i] = new FieldOrder(decs[i], asc);

    }

    IndexDefinition indexDef = new IndexDefinition(metaData, fields, orders);

    indexDefs.add(indexDef);
  }

}
