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

import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;

import com.google.common.collect.Lists;

import at.molindo.esi4j.action.MultiGetItemResponseWrapper;
import at.molindo.esi4j.action.MultiGetItemResponseWrapper.MultiGetItemReader;
import at.molindo.esi4j.action.MultiGetResponseWrapper;

public class DefaultMultiGetResponseWrapper implements MultiGetResponseWrapper {

	private final MultiGetResponse _response;
	private final MultiGetItemReader _reader;

	private List<MultiGetItemResponseWrapper> _objects;

	public DefaultMultiGetResponseWrapper(final MultiGetResponse response, final MultiGetItemReader reader) {
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
	public synchronized List<MultiGetItemResponseWrapper> getMultiGetItemResponses() {
		if (_objects == null) {
			final MultiGetItemResponse[] reps = _response.getResponses();
			_objects = Lists.newArrayListWithCapacity(reps.length);
			for (final MultiGetItemResponse rep : reps) {
				_objects.add(new DefaultMultiGetItemResponseWrapper(rep, _reader));
			}
		}
		return _objects;
	}

	@Override
	public MultiGetResponse getMultiGetResponse() {
		return _response;
	}

	@Override
	public List<?> getObjects() {
		return getObjects(Object.class);
	}

	@Override
	public <T> List<T> getObjects(final Class<T> type) {
		final List<MultiGetItemResponseWrapper> resps = getMultiGetItemResponses();
		final List<T> objects = Lists.newArrayListWithCapacity(resps.size());

		for (final MultiGetItemResponseWrapper hit : resps) {
			objects.add(hit.getObject(type));
		}

		return objects;
	}

}
