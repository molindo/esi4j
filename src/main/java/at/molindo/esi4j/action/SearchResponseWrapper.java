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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import at.molindo.esi4j.mapping.TypeMapping;

/**
 * wraps a {@link SearchResponse}, allows to get returned results as a
 * {@link List} of {@link SearchHitWrapper}
 */
public interface SearchResponseWrapper {

	SearchResponse getSearchResponse();

	List<SearchHitWrapper> getSearchHits();

	/**
	 * @see #getObjects(Class)
	 */
	List<?> getObjects();

	/**
	 * @return may contain <code>null</code> if
	 *         {@link TypeMapping#read(SearchHit)} returns <code>null</code>
	 */
	<T> List<T> getObjects(Class<T> type);

	long getTotalHits();

}
