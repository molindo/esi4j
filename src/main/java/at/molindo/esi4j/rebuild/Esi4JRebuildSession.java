/**
 * Copyright 2010 Molindo GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package at.molindo.esi4j.rebuild;

import java.util.List;

import at.molindo.esi4j.rebuild.scrutineer.ScrutineerRebuildProcessor;

/**
 * Session management for rebuilds. Sessions must always be started before any
 * change to the index.
 */
public interface Esi4JRebuildSession {

	/**
	 * @return true if session is ordered by id (and thus suited for
	 *         {@link ScrutineerRebuildProcessor})
	 */
	boolean isOrdered();

	/**
	 * @return the type this session is for
	 */
	Class<?> getType();

	/**
	 * Must only be called after all elements of previous batch haven been
	 * processed. Therefore, previous state may be cleared during any
	 * invocation.
	 * 
	 * @param batchSize
	 * @return up to the given number of elements. empty list if no more data
	 *         available
	 */
	List<?> getNext(int batchSize);

	/**
	 * @return an optional metadata object that will be written to index after
	 *         successful rebuild. May be <code>null</code>. Metadata type must
	 *         be mapped.
	 */
	Object getMetadata();

	/**
	 * release resources. Must always be called, even in case of errors.
	 */
	void close();

}
