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
package com.datastax.hectorjpa.serialize;

import java.nio.ByteBuffer;

/**
 * Simple interface to allow users to create their own serializaton schemes for JPA to use
 * 
 * @author Todd Nine
 *
 */
public interface EmbeddedSerializer {

	/**
	 * Get the bytes for the serialized object
	 * @param value
	 * @return
	 */
	public ByteBuffer getBytes(Object value);
	
	
	/**
	 * Get the object for the bytes
	 * @param <T>
	 * @param bytes
	 * @return
	 */
	public <T> T getObject(ByteBuffer bytes);
	
	
	
	
}
