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

import com.google.common.collect.Lists;

import at.molindo.esi4j.action.SearchHitWrapper;
import at.molindo.esi4j.action.SearchHitWrapper.SearchHitReader;
import at.molindo.esi4j.action.SearchResponseWrapper;

public class DefaultSearchResponseWrapper implements SearchResponseWrapper {

	private final SearchResponse _response;
	private final SearchHitReader _reader;

	private List<SearchHitWrapper> _objects;

	public DefaultSearchResponseWrapper(final SearchResponse response, final SearchHitReader reader) {
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
	public synchronized List<SearchHitWrapper> getSearchHits() {
		if (_objects == null) {
			final SearchHit[] hits = _response.getHits().hits();
			_objects = Lists.newArrayListWithCapacity(hits.length);
			for (final SearchHit hit : hits) {
				_objects.add(new DefaultSearchHitWrapper(hit, _reader));
			}
		}
		return _objects;
	}

	@Override
	public SearchResponse getSearchResponse() {
		return _response;
	}

	@Override
	public List<?> getObjects() {
		return getObjects(Object.class);
	}

	@Override
	public <T> List<T> getObjects(final Class<T> type) {
		final List<SearchHitWrapper> hits = getSearchHits();
		final List<T> objects = Lists.newArrayListWithCapacity(hits.size());

		for (final SearchHitWrapper hit : hits) {
			objects.add(hit.getObject(type));
		}

		return objects;
	}

	@Override
	public long getTotalHits() {
		return getSearchResponse().getHits().getTotalHits();
	}

}
