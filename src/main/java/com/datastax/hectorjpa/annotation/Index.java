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
 * Interface for indexing a field and all associated order.  Note that every order by must use this 
 * 
 * @author Todd Nine
 * 
 */

@Target({ METHOD, FIELD })
@Retention(RUNTIME)
public @interface Index {

  String value() default "";

}
