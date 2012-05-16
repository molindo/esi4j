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

import org.elasticsearch.search.SearchHit;

import at.molindo.esi4j.action.SearchHitWrapper;

public final class DefaultSearchHitWrapper implements SearchHitWrapper {

	private final SearchHit _hit;
	private final SearchHitReader _reader;

	private Object _object;

	public DefaultSearchHitWrapper(SearchHit hit, SearchHitReader reader) {
		if (hit == null) {
			throw new NullPointerException("hit");
		}
		if (reader == null) {
			throw new NullPointerException("reader");
		}
		_hit = hit;
		_reader = reader;
	}

	@Override
	@SuppressWarnings("unchecked")
	public synchronized <T> T getObject() {
		if (_object == null) {
			_object = _reader.read(_hit);
		}

		return (T) _object;
	}

	@Override
	public <T> T getObject(Class<T> type) {
		return type.cast(getObject());
	}

	@Override
	public SearchHit getHit() {
		return _hit;
	}

}