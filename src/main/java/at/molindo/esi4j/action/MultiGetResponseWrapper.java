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

import java.util.List;

import org.elasticsearch.action.get.MultiGetResponse;

/**
 * wraps a {@link MultiGetResponsee}, allows to get returned results as a
 * {@link List} of {@link MultiGetItemResponseWrapper} (similar to
 * {@link SearchResponseWrapper})
 */
public interface MultiGetResponseWrapper {

	MultiGetResponse getMultiGetResponse();

	List<MultiGetItemResponseWrapper> getMultiGetItemResponses();

	/**
	 * @see #getObjects(Class)
	 */
	List<?> getObjects();

	/**
	 * @return {@link List} of objects of cast to given type. may contain
	 *         <code>null</code> if ID does not exist or
	 *         {@link TypeMapping#read(GetResponse))} returns <code>null</code>
	 */
	<T> List<T> getObjects(Class<T> type);

}
