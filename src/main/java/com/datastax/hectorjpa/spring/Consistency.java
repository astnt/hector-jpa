package com.datastax.hectorjpa.spring;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * Annotates methods and sets the consistency level on the current thread of execute for all callers.
 * 
 * @author Todd Nine
 *
 */
@Documented
@Target({ METHOD })
@Retention(RUNTIME)
public @interface Consistency {
	
	ConsistencyLevel value();

}

