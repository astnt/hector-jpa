package com.datastax.hectorjpa.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.openjpa.kernel.exps.CandidatePath;
import org.apache.openjpa.kernel.exps.Expression;
import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.apache.openjpa.kernel.exps.Literal;
import org.apache.openjpa.kernel.exps.Value;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;

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

  private List<IndexQuery> queries = new ArrayList<IndexQuery>();
  private int currentIndex;
  private CassandraClassMetaData classMetaData;
  
  private FieldMetaData field;
  private Object value;
  
  

  public IndexExpressionVisitor(CassandraClassMetaData classMetaData) {
    this.classMetaData = classMetaData;
    
    this.queries.add(new IndexQuery(this.classMetaData));
    this.currentIndex = 0;
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
      FieldExpression field = getFieldExpression();
      field.setStart(value, true);
      field.setEnd(value, true);
      
      return;
    }
    
    
    if(exp instanceof LessThanEqualExpression){
      FieldExpression field = getFieldExpression();
      field.setEnd(value, true);
      
      return;
    }
    
    if(exp instanceof LessThanExpression){
      FieldExpression field = getFieldExpression();
      field.setEnd(value, false);
      
      return;
    }
    
    if(exp instanceof GreaterThanEqualExpression){
      FieldExpression field = getFieldExpression();
      field.setStart(value, true);
      
      return;
    }
    
    if(exp instanceof GreaterThanExpression){
      FieldExpression field = getFieldExpression();
      field.setStart(value, false);
      
      return;
    }
    

  }

  @Override
  public void enter(Value val) {
    // it's a variable add it to the field
    if (val instanceof CandidatePath) {
      field = ((CandidatePath) val).last();

      return;

    }

    if (val instanceof Literal) {
      value = ((Literal)val).getValue();
    }

  }

  @Override
  public void exit(Value val) {
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
