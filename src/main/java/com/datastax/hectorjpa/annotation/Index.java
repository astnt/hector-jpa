/**
 * 
 */
package com.datastax.hectorjpa.annotation;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Interface for indexing a field and all associated ordering.  Note that every order by must use this.  
 * 
 *  For instance, if you query the email field and order by firstName then lastName, as well as by lastLoginDate, you would need these annotation.  Note that multiple fields can be combined in an index.
 *  The only distinguishing feature requiring a new index annotation is a change in ordering.
 *  
 *  <pre>
 *  @Index(fields="email", order="firstname, lastName, lastLoginDate desc")
 *  <pre>
 * 
 * @author Todd Nine
 * 
 */

@Target(TYPE)
@Retention(RUNTIME)
public @interface Index {

  String fields() default "";
  
  String order() default "";

}
