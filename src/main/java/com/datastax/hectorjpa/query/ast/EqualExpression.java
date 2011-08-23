/**
 * 
 */
package com.datastax.hectorjpa.query.ast;

import org.apache.openjpa.kernel.exps.Value;

/**
 * @author Todd Nine
 * 
 */
public class EqualExpression extends EqualityExpression {


  /**
   * 
   */
  private static final long serialVersionUID = -3010654445901176248L;

  public EqualExpression(Value parameter, Value value) {
    super(parameter, value);
  }



}
