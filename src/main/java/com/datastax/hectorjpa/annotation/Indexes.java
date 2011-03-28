/**
 * 
 */
package com.datastax.hectorjpa.annotation;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Annotation for defining multiple indexes on an object
 * 
 * <pre>
 *  @Indexes({
 *  @Index(fields="firstName, lastName", order="lastLoginDate desc")
 *  @Index(fields="lastLoginDate", order="firstName, lastName")
 *  }
 * 
 * <pre>
 * 
 * @author Todd Nine
 * 
 */

@Target(TYPE)
@Retention(RUNTIME)
public @interface Indexes {

  public Index[] value();

}
