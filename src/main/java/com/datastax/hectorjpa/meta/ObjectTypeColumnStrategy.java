/**
 * 
 */
package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * Represents a column that will always be written regardless of the Entity data.  
 * 
 * @author Todd Nine
 *
 */
public interface ObjectTypeColumnStrategy {

	/**
	 * Write the value
	 * @param mutator
	 * @param clock
	 * @param key
	 * @param cfName
	 */
	public void write(Mutator<byte[]> mutator, long clock, byte[] key, String cfName);


	/**
	 * Get get the stored type.  If no row is present, null is returned.  Otherwise 
	 * a valid string must be returned which will be passed to getClass
	 * 
	 * @return
	 */
	public String getStoredType(Object rowKey, String cfName, Keyspace keyspace);
	
	/**
	 * Get the class for the persisted value.
	 * @param value
	 * @param candidate
	 * @return
	 */
	public Class<?> getClass(String value, Class<?> candidate, MetaCache metaCache);
}
