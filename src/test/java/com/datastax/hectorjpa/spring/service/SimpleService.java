/**
 * 
 */
package com.datastax.hectorjpa.spring.service;

import com.datastax.hectorjpa.spring.model.Model;


/**
 * Simple service test framework
 * 
 * @author Todd Nine
 *
 */
public interface SimpleService {

	/**
	 * 
	 * @param m
	 */
	public void doOp(Model m);
	
	/**
	 * Throws a runtime exception
	 * @param m
	 */
	public void doOpWithException(Model m);
	
	/**
	 * 
	 * @return
	 */
	public boolean isDoOp();
	
	
}
