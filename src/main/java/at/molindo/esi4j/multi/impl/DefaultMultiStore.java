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
package at.molindo.esi4j.multi.impl;

import java.util.Iterator;

import at.molindo.esi4j.core.Esi4JClient;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.Esi4JStore;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.multi.Esi4JManagedMultiIndex;
import at.molindo.esi4j.multi.Esi4JMultiStore;

public class DefaultMultiStore implements Esi4JMultiStore {

	private final Esi4JManagedMultiIndex _index;
	private final Iterable<Esi4JStore> _stores;

	public DefaultMultiStore(Esi4JManagedMultiIndex index) {
		if (index == null) {
			throw new NullPointerException("index");
		}
		_index = index;
		_stores = new Iterable<Esi4JStore>() {

			@Override
			public Iterator<Esi4JStore> iterator() {
				return gestStoresIterator();
			}
		};
	}

	@Override
	public String getIndexName() {
		StringBuilder buf = new StringBuilder();
		for (Esi4JStore store : _stores) {
			buf.append(store.getIndexName()).append(",");
		}
		buf.setLength(buf.length() - 1);
		return buf.toString();
	}

	@Override
	public <T> T execute(StoreOperation<T> operation) {
		return operation.execute(getClient().getClient(), getIndexName());
	}

	@Override
	public void setIndex(Esi4JIndex index) {
		throw new IllegalStateException("store was already assigned to an index named " + index.getName());
	}

	@Override
	public Esi4JClient getClient() {
		Esi4JClient client = null;
		for (Esi4JStore store : _stores) {
			if (client == null) {
				client = store.getClient();
			} else if (!client.getClusterName().equals(store.getClient().getClusterName())) {
				throw new IllegalArgumentException("can't create store among multiple clusters");
			}
		}
		return client;
	}

	@Override
	public Iterable<Esi4JStore> getStores() {
		return _stores;
	}

	private Iterator<Esi4JStore> gestStoresIterator() {
		return new Iterator<Esi4JStore>() {

			private final Iterator<InternalIndex> _iter = _index.getIndices().values().iterator();

			@Override
			public Esi4JStore next() {
				return _iter.next().getStore();
			}

			@Override
			public boolean hasNext() {
				return _iter.hasNext();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public void close() {
	}

}
