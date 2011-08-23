/**
 * 
 */
package com.datastax.hectorjpa.query;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.Keyspace;

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

import com.datastax.hectorjpa.index.AbstractIndexOperation;
import com.datastax.hectorjpa.index.FieldOrder;
import com.datastax.hectorjpa.index.IndexOrder;
import com.datastax.hectorjpa.query.field.FieldExpression;
import com.datastax.hectorjpa.query.iterator.ResultCompiler;
import com.datastax.hectorjpa.store.CassandraClassMetaData;
import com.datastax.hectorjpa.store.CassandraStoreConfiguration;
import com.datastax.hectorjpa.store.EntityFacade;

/**
 * Cassandra specific query implementation
 * 
 * @author Todd Nine
 * 
 */
public class CassandraStoreQuery extends ExpressionStoreQuery {

  private static final Logger log = LoggerFactory
      .getLogger(CassandraStoreQuery.class);

  private CassandraStoreConfiguration conf;

  /**
   * 
   */
  private static final long serialVersionUID = -756133912086570146L;

  public CassandraStoreQuery(ExpressionParser parser, CassandraStoreConfiguration conf) {
    super(parser);
    this.conf = conf;
  }

  @Override
  public boolean supportsDataStoreExecution() {
    return true;
  }

  @Override
  protected ResultObjectProvider executeQuery(Executor ex, ClassMetaData base,
      ClassMetaData[] types, boolean subclasses, ExpressionFactory[] facts,
      QueryExpressions[] parsed, Object[] params, Range range) {
    
	 log.debug("In executeQuery");
  
    IndexExpressionVisitor visitor = new IndexExpressionVisitor(
        (CassandraClassMetaData) base, params);

    parsed[0].filter.acceptVisit(visitor);

    // the list of queries we need to execute
    List<IndexQuery> queries = visitor.getVisitors();


    
    AbstractIndexOperation indexOp = getIndexOp(queries.get(0), parsed[0].ordering, parsed[0].ascending, (CassandraClassMetaData) base);

    
    ResultCompiler compiler = new ResultCompiler(indexOp.getComprator());
 
    Keyspace keyspace = conf.getKeyspace();
    
    compiler.addScanIterator(indexOp.scanIndex(queries.get(0), keyspace));
    
    //loop throught and add all other clauses
    for(int i = 1; i < queries.size(); i ++){
      log.debug("indexOp: {} and base: {} with range: {}", new Object[]{indexOp, base,range});
      compiler.addScanIterator(indexOp.scanIndex(queries.get(i), keyspace));
    }
    
    int start = 0;
    int size = Integer.MAX_VALUE;
    
    if(range.start != 0 || range.end != Long.MAX_VALUE){
      start = (int) range.start;
      size = (int) (range.end - range.start);
      //this is a limitation in cassandra paging, more than an int isn't supported for loading columns.  Not really an issue since that many rows propbably can't be loaded in ram anyway
    }
    
    
    //TODO T.N. incorporate LRS
    compiler.compile(start, size);
    
    CassandraResultObjectProvider results = new CassandraResultObjectProvider(
        compiler.getResults(), this.getContext().getStoreContext(),
        ctx.getFetchConfiguration(), (CassandraClassMetaData) base);

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
      boolean[] orderAscending, CassandraClassMetaData metaData) {

    Collection<FieldExpression> expFields = query.getExpressions();

    FieldOrder[] fields = new FieldOrder[expFields.size()];

    IndexOrder[] orders = new IndexOrder[ordering.length];

    Iterator<FieldExpression> expFieldsItr = expFields.iterator();

    // all fields are ascending by default in field equality.
    for (int i = 0; i < fields.length; i++) {
      FieldExpression current = expFieldsItr.next();

      // if there's not start but an end then this column must be
      // descending in
      // the index

      fields[i] = new FieldOrder(current.getField().getName(), true);
      log.debug("in getIndexOp with field: {}", fields[i]);
    }

    for (int i = 0; i < orders.length; i++) {
      orders[i] = new IndexOrder(((Path) ordering[i]).last().getName(),
          orderAscending[i]);
    }

    EntityFacade classMeta = conf.getMetaCache().getFacade(metaData, conf.getSerializer());

    AbstractIndexOperation indexOp = classMeta
        .getIndexOperation(fields, orders);

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

    if (orders.length > 1) {
      buff.append("\" order=\"");

      for (int i = 0; i < orders.length; i++) {
        buff.append(orders[i].getName());

        if (!orders[i].isAscending()) {
          buff.append(" desc");
        }

        buff.append(",");
      }

      buff.setLength(buff.length() - 1);
    }
    buff.append("\")");

    return buff.toString();
  }

}
