/**
 * 
 */
package com.datastax.hectorjpa.spring.service;

import static org.junit.Assert.assertEquals;
import me.prettyprint.hector.api.HConsistencyLevel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.datastax.hectorjpa.consitency.JPAConsistency;
import com.datastax.hectorjpa.spring.Consistency;
import com.datastax.hectorjpa.spring.model.Model;
import com.datastax.hectorjpa.spring.model.ModelChild;

/**
 * @author Todd Nine
 * 
 */
@Service
public class ComplexServiceImpl implements ComplexService {

	@Autowired
	private SimpleService s;

	@Autowired
	private SimpleServiceTwo s2;

	private boolean doOp;

	private boolean doOpChild;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.spidertracks.datanucleus.spring.ComplexService#doOp(com.spidertracks
	 * .datanucleus.spring.model.Model)
	 */
	@Override
	@Consistency(HConsistencyLevel.ONE)
	public void doOp(Model m) {
		assertEquals(HConsistencyLevel.ONE,
				JPAConsistency.get());

		s.doOp(m);

		assertEquals(HConsistencyLevel.ONE,
				JPAConsistency.get());

		s2.doOp(m);

		doOp = true;
		doOpChild = false;

		assertEquals(HConsistencyLevel.ONE,
				JPAConsistency.get());

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.spidertracks.datanucleus.spring.ComplexService#doOp(com.spidertracks
	 * .datanucleus.spring.model.ModelChild)
	 */
	@Override
	@Consistency(HConsistencyLevel.EACH_QUORUM)
	public void doOp(ModelChild m) {
		assertEquals(HConsistencyLevel.EACH_QUORUM,
				JPAConsistency.get());

		s.doOp(m);

		assertEquals(HConsistencyLevel.EACH_QUORUM,
				JPAConsistency.get());

		s2.doOp(m);

		assertEquals(HConsistencyLevel.EACH_QUORUM,
				JPAConsistency.get());

		doOp = false;
		doOpChild = true;

	}

	@Override
	@Consistency(HConsistencyLevel.EACH_QUORUM)
	public void doOpChildException(ModelChild m) {
		
		assertEquals(HConsistencyLevel.EACH_QUORUM,
				JPAConsistency.get());

		s2.doOp(m);

		assertEquals(HConsistencyLevel.EACH_QUORUM,
				JPAConsistency.get());

		
		s.doOpWithException(m);

		//ensure our state is rest after the exception is thrown
	
		

	}

	/**
	 * @return the doOp
	 */
	public boolean isDoOp() {
		return doOp;
	}

	/**
	 * @return the doOpChild
	 */
	public boolean isDoOpChild() {
		return doOpChild;
	}

}
