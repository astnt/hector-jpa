/**
 * 
 */
package com.datastax.hectorjpa.spring.service;

import com.datastax.hectorjpa.spring.model.Model;


/**
 * @author Todd Nine
 *
 */
public interface SimpleServiceTwo {

	/**
	 * 
	 * @param m
	 */
	public void doOp(Model m);
	
	/**
	 * 
	 * @return
	 */
	public boolean isDoOp();
	
}
