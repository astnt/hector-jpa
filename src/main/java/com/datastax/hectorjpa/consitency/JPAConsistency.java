/**********************************************************************
Copyright (c) 2010 Todd Nine. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
 ***********************************************************************/
package com.datastax.hectorjpa.consitency;

import me.prettyprint.hector.api.HConsistencyLevel;

/**
 * Class that is used similar to a transaction to set the consistency level for
 * a given operation. If none is set on the thread, HConsistencyLevel.ONE is used as the default
 * 
 * 
 * @author Todd Nine
 * 
 */
public class JPAConsistency {

	private static final ThreadLocal<HConsistencyLevel> level = new ThreadLocal<HConsistencyLevel>();
	
	private static HConsistencyLevel defaultLevel;

	
	static {
		defaultLevel = HConsistencyLevel.QUORUM;
	}
	
	/**
	 * Set the default level for all threads
	 * @param c
	 */
	public static void setDefault(HConsistencyLevel c){
		defaultLevel = c;
	}
	
	/**
	 * Set the consistency level for this thread
	 * 
	 * @param c
	 */
	public static void set(HConsistencyLevel c) {
		level.set(c);
	}

	
	/**
	 * Convenience wrapper for set(null) which invokes the default
	 */
	public static void remove() {
		set(null);
	}

	/**
	 * Get the currently set consistency level;
	 * 
	 * @return
	 */
	public static HConsistencyLevel get() {
	  HConsistencyLevel l = level.get();

		if (l == null) {
			return defaultLevel;
		}

		return l;
	}

}
