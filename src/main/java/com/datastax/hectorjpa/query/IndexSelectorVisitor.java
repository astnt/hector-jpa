/**
 * 
 */
package com.datastax.hectorjpa.query;

import java.util.HashSet;
import java.util.Set;

import org.apache.openjpa.jdbc.kernel.exps.Param;
import org.apache.openjpa.kernel.exps.CandidatePath;
import org.apache.openjpa.kernel.exps.Expression;
import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.apache.openjpa.kernel.exps.Value;
import org.apache.openjpa.meta.FieldMetaData;

/**
 * A visitor that collects all the fields in our expression tree to be used to determine which
 * index to utilize
 * 
 * @author Todd Nine
 *
 */
public class IndexSelectorVisitor implements ExpressionVisitor {
  
  private Set<FieldMetaData> fields = new HashSet<FieldMetaData>();

  
  /* (non-Javadoc)
   * @see org.apache.openjpa.kernel.exps.ExpressionVisitor#enter(org.apache.openjpa.kernel.exps.Expression)
   */
  @Override
  public void enter(Expression exp) {
  }

  /* (non-Javadoc)
   * @see org.apache.openjpa.kernel.exps.ExpressionVisitor#exit(org.apache.openjpa.kernel.exps.Expression)
   */
  @Override
  public void exit(Expression exp) {
  }

  /* (non-Javadoc)
   * @see org.apache.openjpa.kernel.exps.ExpressionVisitor#enter(org.apache.openjpa.kernel.exps.Value)
   */
  @Override
  public void enter(Value val) {
    if(!(val instanceof CandidatePath)){
      return;
    }
    
    FieldMetaData fmd = ((CandidatePath)val).last();
    
    fields.add(fmd);
    
  }

  /* (non-Javadoc)
   * @see org.apache.openjpa.kernel.exps.ExpressionVisitor#exit(org.apache.openjpa.kernel.exps.Value)
   */
  @Override
  public void exit(Value val) {
  }

  public Set<FieldMetaData> getFields(){
    return fields;
  }
}
