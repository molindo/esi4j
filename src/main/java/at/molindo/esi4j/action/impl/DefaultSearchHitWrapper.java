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
import at.molindo.esi4j.core.internal.InternalIndex;

public final class DefaultSearchHitWrapper implements SearchHitWrapper {

	private final SearchHit _hit;
	private final InternalIndex _index;

	private Object _object;

	public DefaultSearchHitWrapper(SearchHit hit, InternalIndex index) {
		if (hit == null) {
			throw new NullPointerException("hit");
		}
		if (index == null) {
			throw new NullPointerException("index");
		}
		_hit = hit;
		_index = index;
	}

	@Override
	@SuppressWarnings("unchecked")
	public synchronized <T> T getObject() {
		if (_object == null) {
			_object = _index.read(_hit);
		}

		return (T) _object;
	}

	@Override
	public <T> T getObject(Class<T> type) {
		return type.cast(getObject());
	}

	public SearchHit getHit() {
		return _hit;
	}

}