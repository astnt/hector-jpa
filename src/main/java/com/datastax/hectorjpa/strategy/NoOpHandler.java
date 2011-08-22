/**
 * 
 */
package com.datastax.hectorjpa.strategy;

import java.sql.SQLException;

import org.apache.openjpa.jdbc.kernel.JDBCFetchConfiguration;
import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.jdbc.meta.ValueHandler;
import org.apache.openjpa.jdbc.meta.ValueMapping;
import org.apache.openjpa.jdbc.schema.Column;
import org.apache.openjpa.jdbc.schema.ColumnIO;
import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Simple value handler for handing values that the Cassandra plugin can correctly serialize, but is not natively understood by the 
 * open JPA framework.  Useful for such classes as com.eaio.uuid.UUID when used as a persistent field. 
 * @author Todd Nine
 *
 */
public class NoOpHandler implements ValueHandler {

	private static final Logger logger = LoggerFactory.getLogger(NoOpHandler.class);
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -396031782093695953L;

	@Override
	public Column[] map(ValueMapping vm, String name, ColumnIO io, boolean adapt) {
		return null;
	}

	@Override
	public boolean isVersionable(ValueMapping vm) {
		return false;
	}

	@Override
	public boolean objectValueRequiresLoad(ValueMapping vm) {
		return false;
	}

	@Override
	public Object getResultArgument(ValueMapping vm) {
		return null;
	}

	@Override
	public Object toDataStoreValue(ValueMapping vm, Object val, JDBCStore store) {
		logger.debug("Passing through to data store value val: {}", val);
		return val;
	}

	@Override
	public Object toObjectValue(ValueMapping vm, Object val) {
		logger.debug("Passing through to object value val: {}", val);
		return val;
	}

	@Override
	public Object toObjectValue(ValueMapping vm, Object val,
			OpenJPAStateManager sm, JDBCStore store,
			JDBCFetchConfiguration fetch) throws SQLException {
		
		logger.debug("Passing through to object value val: {}", val);
		return val;
	}


}
