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
package at.molindo.esi4j.rebuild.util;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import at.molindo.esi4j.rebuild.Esi4JRebuildSession;

/**
 * {@link Esi4JRebuildSession} implementation using a simple {@link Iterator}
 */
public class IteratorRebuildSession implements Esi4JRebuildSession {

	private final Class<?> _type;
	private Iterator<?> _iterator;
	private final boolean _ordered;

	public IteratorRebuildSession(final Class<?> type, final Iterator<?> iterator, final boolean ordered) {
		if (type == null) {
			throw new NullPointerException("type");
		}
		if (iterator == null) {
			throw new NullPointerException("iterator");
		}
		_type = type;
		_iterator = iterator;
		_ordered = ordered;
	}

	@Override
	public Class<?> getType() {
		return _type;
	}

	@Override
	public boolean isOrdered() {
		return _ordered;
	}

	@Override
	public List<?> getNext(final int batchSize) {
		if (_iterator == null) {
			throw new IllegalStateException("already closed");
		}

		final List<Object> list = Lists.newArrayListWithCapacity(batchSize);
		while (list.size() < batchSize && _iterator.hasNext()) {
			list.add(_iterator.next());
		}

		return list;
	}

	@Override
	public Object getMetadata() {
		return null;
	}

	@Override
	public void close() {
		_iterator = null;
	}

}
