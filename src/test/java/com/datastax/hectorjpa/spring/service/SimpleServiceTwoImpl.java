/**
 * 
 */
package com.datastax.hectorjpa.spring.service;

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.springframework.stereotype.Service;

import com.datastax.hectorjpa.consitency.JPAConsistency;
import com.datastax.hectorjpa.spring.Consistency;
import com.datastax.hectorjpa.spring.model.Model;

/**
 * @author Todd Nine
 * 
 */
@Service
public class SimpleServiceTwoImpl implements SimpleServiceTwo {

	boolean doOp;

	@Override
	@Consistency(ConsistencyLevel.QUORUM)
	public void doOp(Model m) {
		assertEquals(ConsistencyLevel.QUORUM,
				JPAConsistency.get());

		doOp = true;

	}

	@Override
	public boolean isDoOp() {
		return doOp;
	}


}
