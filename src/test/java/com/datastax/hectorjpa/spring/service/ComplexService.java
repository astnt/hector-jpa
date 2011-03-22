/**
 * 
 */
package com.datastax.hectorjpa.spring.service;

import com.datastax.hectorjpa.spring.model.Model;
import com.datastax.hectorjpa.spring.model.ModelChild;

/**
 * @author Todd Nine
 *
 */
public interface ComplexService {

	/**
	 * 
	 * @param m
	 */
	public void doOp(Model m);
	
	/**
	 * 
	 * @param m
	 */
	public void doOp(ModelChild m);
	
	
	public void doOpChildException(ModelChild m);
	
	/**
	 * @return the doOp
	 */
	public boolean isDoOp();
	
	/**
	 * 
	 * @return
	 */
	public boolean isDoOpChild();
	

}
