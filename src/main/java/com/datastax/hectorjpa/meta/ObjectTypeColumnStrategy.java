/**
 * 
 */
package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
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
	 * Get the objectId from the system.  Null if the entity doesn't exist
	 * 
	 * @return
	 */
	public String getObjectId(Object rowKey, String cfName, Keyspace keyspace);
	
	/**
	 * Get the class for the persisted value
	 * @param value
	 * @param candidate
	 * @return
	 */
	public Class<?> getClass(String value, Class<?> candidate, MetaCache metaCache);
}
