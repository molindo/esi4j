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
package at.molindo.esi4j.action.impl;

import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import at.molindo.esi4j.action.SearchHitWrapper;
import at.molindo.esi4j.action.SearchHitWrapper.SearchHitReader;
import at.molindo.esi4j.action.SearchResponseWrapper;

import com.google.common.collect.Lists;

public class DefaultSearchResponseWrapper implements SearchResponseWrapper {

	private final SearchResponse _response;
	private final SearchHitReader _reader;

	private List<SearchHitWrapper> _objects;

	public DefaultSearchResponseWrapper(SearchResponse response, SearchHitReader reader) {
		if (response == null) {
			throw new NullPointerException("response");
		}
		if (reader == null) {
			throw new NullPointerException("reader");
		}
		_response = response;
		_reader = reader;
	}

	@Override
	public synchronized List<SearchHitWrapper> getObjects() {
		if (_objects == null) {
			SearchHit[] hits = _response.hits().hits();
			_objects = Lists.newArrayListWithCapacity(hits.length);
			for (int i = 0; i < hits.length; i++) {
				_objects.add(new DefaultSearchHitWrapper(hits[i], _reader));
			}
		}
		return _objects;
	}

	@Override
	public SearchResponse getSearchResponse() {
		return _response;
	}

}
