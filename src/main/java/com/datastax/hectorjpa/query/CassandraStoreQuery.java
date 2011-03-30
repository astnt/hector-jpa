/**
 * 
 */
package com.datastax.hectorjpa.query;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.openjpa.kernel.ExpressionStoreQuery;
import org.apache.openjpa.kernel.exps.ExpressionFactory;
import org.apache.openjpa.kernel.exps.ExpressionParser;
import org.apache.openjpa.kernel.exps.Path;
import org.apache.openjpa.kernel.exps.QueryExpressions;
import org.apache.openjpa.kernel.exps.Value;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.util.UnsupportedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.index.FieldOrder;
import com.datastax.hectorjpa.index.IndexOrder;
import com.datastax.hectorjpa.meta.AbstractIndexOperation;
import com.datastax.hectorjpa.meta.IndexOperation;
import com.datastax.hectorjpa.meta.MetaCache;
import com.datastax.hectorjpa.store.CassandraClassMetaData;
import com.datastax.hectorjpa.store.CassandraStore;

/**
 * Cassandra specific query implementation
 * 
 * @author Todd Nine
 * 
 */
public class CassandraStoreQuery extends ExpressionStoreQuery {

  private static final Logger log = LoggerFactory
      .getLogger(CassandraStoreQuery.class);

  private MetaCache metaCache;

  private CassandraStore store;

  /**
   * 
   */
  private static final long serialVersionUID = -756133912086570146L;

  public CassandraStoreQuery(ExpressionParser parser, MetaCache metaCache,
      CassandraStore store) {
    super(parser);
    this.metaCache = metaCache;
    this.store = store;
    this.store.open();
  }

  @Override
  public boolean supportsDataStoreExecution() {
    return true;
  }

  @Override
  protected ResultObjectProvider executeQuery(Executor ex, ClassMetaData base,
      ClassMetaData[] types, boolean subclasses, ExpressionFactory[] facts,
      QueryExpressions[] parsed, Object[] params, Range range) {

 
    IndexExpressionVisitor visitor = new IndexExpressionVisitor(
        (CassandraClassMetaData) base);

    parsed[0].filter.acceptVisit(visitor);

    // the list of queries we need to execute
    List<IndexQuery> queries = visitor.getVisitors();

    Set<DynamicComposite> columnResults = null;

    AbstractIndexOperation indexOp = null;

    // TODO TN, this is a mess, comparator operations for index ops aren't
    // properly setup in the class structure. Refactor to fix this

    for (IndexQuery query : queries) {

      indexOp = getIndexOp(query, parsed[0].ordering, parsed[0].ascending);

      if (columnResults == null) {
        columnResults = new TreeSet<DynamicComposite>(indexOp.getComprator());
      }

      // we have an index operation, now get the columns from it
      indexOp.scanIndex(query, columnResults, store.getKeyspace());

    }

   
    // now use our limit to remove items we don't need
//TODO TN
    if(range.start != -1){
      
    }
    
    
    
    CassandraResultObjectProvider results = new CassandraResultObjectProvider(columnResults, this.getContext().getStoreContext(), ctx.getFetchConfiguration(), (CassandraClassMetaData) base);


    
    
    
    // TODO Auto-generated method stub

    return results;
  }

  @Override
  protected ExpressionFactory getExpressionFactory(ClassMetaData type) {
    return new CassandraExpressionFactory();
  }

  /**
   * Get the index for the query
   * 
   * @param query
   * @return
   */
  private AbstractIndexOperation getIndexOp(IndexQuery query, Value[] ordering,
      boolean[] orderAscending) {

    Collection<FieldExpression> expFields = query.getExpressions();

    FieldOrder[] fields = new FieldOrder[expFields.size()];

    IndexOrder[] orders = new IndexOrder[ordering.length];

    Iterator<FieldExpression> expFieldsItr = expFields.iterator();

    for (int i = 0; i < fields.length; i++) {
      FieldExpression current = expFieldsItr.next();

      // if there's not start but an end then this column must be descending in
      // the index
      boolean descending = current.getStart() == null
          && current.getEnd() != null;

      fields[i] = new FieldOrder(current.getField().getName(), !descending);
    }

    for (int i = 0; i < orders.length; i++) {
      orders[i] = new IndexOrder(((Path) ordering[i]).last().getName(),
          orderAscending[i]);
    }

    AbstractIndexOperation indexOp = metaCache.getIndexOperation(query.getMetaData(),
        fields, orders);

    if (indexOp == null) {
      throw new UnsupportedException(
          String
              .format(
                  "You attempted to query an index that does not exist.  To perform this query you must define an index in the following format.  '%s'",
                  getIndexExpression(fields, orders)));
    }

    return indexOp;

  }

  /**
   * Return the string representation of the index
   * 
   * @param fields
   * @param orders
   * @return
   */
  private String getIndexExpression(FieldOrder[] fields, IndexOrder[] orders) {
    StringBuffer buff = new StringBuffer();

    buff.append("@Index(fields=\"");

    for (int i = 0; i < fields.length; i++) {
      buff.append(fields[i].getName());

      if (!fields[i].isAscending()) {
        buff.append(" desc");
      }

      buff.append(",");
    }

    buff.setLength(buff.length() - 1);
    buff.append("\" order=\"");

    for (int i = 0; i < orders.length; i++) {
      buff.append(orders[i].getName());

      if (!orders[i].isAscending()) {
        buff.append(" desc");
      }

      buff.append(",");
    }

    buff.setLength(buff.length() - 1);
    buff.append("\")");

    return buff.toString();
  }

}
