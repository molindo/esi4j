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
import at.molindo.esi4j.action.SearchHitWrapper;
import at.molindo.esi4j.action.SearchHitWrapper.SearchHitReader;
import at.molindo.esi4j.action.SearchResponseWrapper;

public class DefaultMultiSearchItemResponseWrapper implements MultiSearchItemResponseWrapper {

	private final MultiSearchResponse.Item _response;
	private final SearchHitReader _reader;

	private DefaultSearchResponseWrapper _responseWrapper;

	public DefaultMultiSearchItemResponseWrapper(final MultiSearchResponse.Item reps, final SearchHitReader reader) {
		if (reps == null) {
			throw new NullPointerException("response");
		}
		if (reader == null) {
			throw new NullPointerException("reader");
		}
		_response = reps;
		_reader = reader;
	}

	@Override
	public MultiSearchResponse.Item getMultiSearchItemResponse() {
		return _response;
	}

	@Override
	public SearchResponseWrapper getSearchResponseWrapper() {
		if (_responseWrapper == null && !_response.isFailure()) {
			_responseWrapper = new DefaultSearchResponseWrapper(_response.getResponse(), _reader);
		}
		return _responseWrapper;
	}

	@Override
	public List<SearchHitWrapper> getSearchHits() {
		return getSearchResponseWrapper().getSearchHits();
	}

	@Override
	public List<?> getObjects() {
		return getSearchResponseWrapper().getObjects();
	}

	@Override
	public <T> List<T> getObjects(final Class<T> type) {
		return getSearchResponseWrapper().getObjects(type);
	}

	@Override
	public long getTotalHits() {
		return getSearchResponseWrapper().getTotalHits();
	}

}
