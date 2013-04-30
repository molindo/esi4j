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
package at.molindo.esi4j.action;

import javax.annotation.CheckForNull;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;

import at.molindo.esi4j.mapping.TypeMapping;

/**
 * wraps a {@link MultiGetItemResponse}, allows to get returned hit as an object
 * (similar to {@link SearchHitWrapper})
 */
public interface MultiGetItemResponseWrapper {

	MultiGetItemResponse getMultiGetItemResponse();

	/**
	 * @return may be <code>null</code> if id not found or
	 *         {@link TypeMapping#read(GetResponse)} returns null
	 */
	@CheckForNull
	Object getObject();

	/**
	 * @return object of given type
	 * @throws ClassCastException
	 *             if returned object is not of given type
	 */
	@CheckForNull
	<T> T getObject(Class<T> type);

	public interface MultiGetItemReader {

		@CheckForNull
		Object read(MultiGetItemResponse response);

	}

}
