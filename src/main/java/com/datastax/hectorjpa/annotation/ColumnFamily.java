/**
 * 
 */
package com.datastax.hectorjpa.annotation;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Class level annotation for declaring a column family
 * 
 * @author Todd Nine
 * 
 */

@Target(TYPE)
@Retention(RUNTIME)
public @interface ColumnFamily {

  String value() default "";

}
