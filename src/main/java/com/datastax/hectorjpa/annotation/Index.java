/**
 * 
 */
package com.datastax.hectorjpa.annotation;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Interface for indexing a field and all associated ordering.  Note that every order by must use this.  
 * 
 *  For instance, if you query the email field and order by firstName then lastName, as well as by lastLoginDate, you would need these annotation.
 *  
 *  <pre>
 *  @Index("firstName, lastName")
 *  @Index("lastLoginDate desc")
 *  private String email;
 *  <pre>
 * 
 * @author Todd Nine
 * 
 */

@Target({ METHOD, FIELD })
@Retention(RUNTIME)
public @interface Index {

  String value() default "";

}
