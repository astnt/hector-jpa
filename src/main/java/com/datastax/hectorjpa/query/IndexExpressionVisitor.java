package com.datastax.hectorjpa.query;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;

import org.apache.openjpa.jdbc.kernel.exps.Param;
import org.apache.openjpa.kernel.exps.CandidatePath;
import org.apache.openjpa.kernel.exps.Expression;
import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.apache.openjpa.kernel.exps.Literal;
import org.apache.openjpa.kernel.exps.Parameter;
import org.apache.openjpa.kernel.exps.Val;
import org.apache.openjpa.kernel.exps.Value;
import org.apache.openjpa.meta.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.query.ast.EqualExpression;
import com.datastax.hectorjpa.query.ast.GreaterThanEqualExpression;
import com.datastax.hectorjpa.query.ast.GreaterThanExpression;
import com.datastax.hectorjpa.query.ast.LessThanEqualExpression;
import com.datastax.hectorjpa.query.ast.LessThanExpression;
import com.datastax.hectorjpa.query.ast.OrExpression;
import com.datastax.hectorjpa.store.CassandraClassMetaData;

/**
 * The expression visitor for index expressions
 * 
 * @author Todd Nine
 * 
 */
public class IndexExpressionVisitor implements ExpressionVisitor {
  private static Logger log = LoggerFactory.getLogger(IndexExpressionVisitor.class);
  private List<IndexQuery> queries = new ArrayList<IndexQuery>();
  private int currentIndex;
  private CassandraClassMetaData classMetaData;
  
  private FieldMetaData field;
  private Object value;
  private Object[] params;
  private int paramIndex;

  public IndexExpressionVisitor(CassandraClassMetaData classMetaData, Object[] params) {
    this.classMetaData = classMetaData;
    
    this.queries.add(new IndexQuery(this.classMetaData));
    this.currentIndex = 0;
    this.params = params;
  }

  @Override
  public void enter(Expression exp) {
    // we've encountered a Or Expression which will require a new query, create
    // one
    if (exp instanceof OrExpression) {
      queries.add(new IndexQuery(this.classMetaData));
      currentIndex++;
      
      return;
    }

  }

  @Override
  public void exit(Expression exp) {

    // we've encountered a Or Expression which required a new query.  Decrement our current index pointer
    if (exp instanceof OrExpression) {
      currentIndex--;
      
      return;
    }
    
    
    if(exp instanceof EqualExpression){
      if (value == null) {
          value = params[paramIndex];
      }
      
      log.debug("in EqualsExpression with {}", value);
      
      FieldExpression field = getFieldExpression();
      field.setStart(value, ComponentEquality.EQUAL);
      //greater than equal is actually inclusive
      field.setEnd(value, ComponentEquality.GREATER_THAN_EQUAL);
      
      return;
    }
    
    
    if(exp instanceof LessThanEqualExpression){
      FieldExpression field = getFieldExpression();
      field.setEnd(value, ComponentEquality.LESS_THAN_EQUAL);
      
      return;
    }
    
    if(exp instanceof LessThanExpression){
      FieldExpression field = getFieldExpression();
      field.setEnd(value, ComponentEquality.EQUAL);
      
      return;
    }
    
    if(exp instanceof GreaterThanEqualExpression){
      log.debug("in GreaterThanEqualsExpression with {}", value);
      FieldExpression field = getFieldExpression();
      field.setStart(value, ComponentEquality.GREATER_THAN_EQUAL);
      
      return;
    }
    
    if(exp instanceof GreaterThanExpression){
      FieldExpression field = getFieldExpression();
      field.setStart(value, ComponentEquality.EQUAL);
      
      return;
    }
    

  }

  @Override
  public void enter(Value val) {
    log.debug("in enter with value {}", val);
    // it's a variable add it to the field
    if (val instanceof CandidatePath) {
      field = ((CandidatePath) val).last();
      log.debug("field name {} and {}", field.getName(), field);
      return;

    }

    if (val instanceof Literal) {
      value = ((Literal)val).getValue();
    } else if (val instanceof Param) {      
      log.debug("reset with value {}", val);
      Param param = (Param)val;
      paramIndex = param.getIndex();
      value = null;
    }
  }

  @Override
  public void exit(Value val) {
    log.debug("in exit with value {}", val);
    // TODO Auto-generated method stub

  }

  /**
   * Get the list of index operations to perform
   * @return
   */
  public List<IndexQuery> getVisitors(){
    return queries;
  }
  
  
  /**
   * Get the field expression for the current field
   * @return
   */
  private FieldExpression getFieldExpression(){
    IndexQuery query = queries.get(currentIndex);
    FieldExpression exp = query.getExpression(field);
    
    if(exp == null){
      exp = new FieldExpression(field);
      query.addExpression(exp);
    }
    
    return exp;
    
  }
}
