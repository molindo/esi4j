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

import org.elasticsearch.action.search.MultiSearchResponse;

import at.molindo.esi4j.action.MultiSearchItemResponseWrapper;
import at.molindo.esi4j.action.MultiSearchResponseWrapper;
import at.molindo.esi4j.action.SearchHitWrapper;
import at.molindo.esi4j.action.SearchHitWrapper.SearchHitReader;

import com.google.common.collect.Lists;

public class DefaultMultiSearchResponseWrapper implements MultiSearchResponseWrapper {

	private final MultiSearchResponse _response;
	private final SearchHitReader _reader;

	private List<MultiSearchItemResponseWrapper> _objects;

	public DefaultMultiSearchResponseWrapper(MultiSearchResponse response, SearchHitReader reader) {
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
	public synchronized List<MultiSearchItemResponseWrapper> getMultiSearchItemResponses() {
		if (_objects == null) {
			MultiSearchResponse.Item[] reps = _response.responses();
			_objects = Lists.newArrayListWithCapacity(reps.length);
			for (int i = 0; i < reps.length; i++) {
				_objects.add(new DefaultMultiSearchItemResponseWrapper(reps[i], _reader));
			}
		}
		return _objects;
	}

	@Override
	public MultiSearchResponse getMultiSearchResponse() {
		return _response;
	}

	@Override
	public List<?> getObjects() {
		return getObjects(Object.class);
	}

	@Override
	public <T> List<T> getObjects(Class<T> type) {
		List<MultiSearchItemResponseWrapper> resps = getMultiSearchItemResponses();

		int size = 0;
		for (MultiSearchItemResponseWrapper response : resps) {
			size += response.getSearchHits().size();
		}

		List<T> objects = Lists.newArrayListWithCapacity(size);

		for (MultiSearchItemResponseWrapper response : resps) {
			objects.addAll(response.getObjects(type));
		}

		return objects;
	}

	@Override
	public List<SearchHitWrapper> getSearchHits() {
		List<MultiSearchItemResponseWrapper> resps = getMultiSearchItemResponses();

		int size = 0;
		for (MultiSearchItemResponseWrapper response : resps) {
			size += response.getSearchHits().size();
		}

		List<SearchHitWrapper> searchHits = Lists.newArrayListWithCapacity(size);

		for (MultiSearchItemResponseWrapper response : resps) {
			searchHits.addAll(response.getSearchHits());
		}

		return searchHits;
	}

}
