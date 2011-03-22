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
public class SimpleServiceImpl implements SimpleService {

	boolean doOp;
	
	@Override
	@Consistency(ConsistencyLevel.LOCAL_QUORUM)
	public void doOp(Model m) {
		assertEquals(ConsistencyLevel.LOCAL_QUORUM, JPAConsistency.get());

		doOp = true;
	}

	@Override
	public boolean isDoOp() {
		return doOp;
	}

	@Override
	@Consistency(ConsistencyLevel.LOCAL_QUORUM)
	public void doOpWithException(Model m) {
		assertEquals(ConsistencyLevel.LOCAL_QUORUM, JPAConsistency.get());

		throw new RuntimeException("Texting exception!");
	}

}
