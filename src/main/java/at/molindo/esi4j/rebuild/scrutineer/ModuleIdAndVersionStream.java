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
package at.molindo.esi4j.rebuild.scrutineer;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.rebuild.Esi4JRebuildSession;
import at.molindo.scrutineer.IdAndVersion;
import at.molindo.scrutineer.IdAndVersionStream;

public final class ModuleIdAndVersionStream implements IdAndVersionStream {

	private final int _batchSize;
	private final TypeMapping _mapping;
	private Esi4JRebuildSession _rebuildSession;

	public ModuleIdAndVersionStream(final Esi4JRebuildSession rebuildSession, final int batchSize, final TypeMapping mapping) {
		_rebuildSession = rebuildSession;
		_batchSize = batchSize;
		_mapping = mapping;
	}

	@Override
	public boolean isSorted() {
		return _rebuildSession.isOrdered();
	}

	@Override
	public void open() {
	}

	@Override
	public Iterator<IdAndVersion> iterator() {
		verifyOpen();
		return new ModuleIdAndVersionStreamIterator();
	}

	@Override
	public void close() {
		_rebuildSession = null;
	}

	final class ModuleIdAndVersionStreamIterator implements Iterator<IdAndVersion> {

		private Iterator<?> _iter;
		private Object _next;

		private boolean _lastBatchFetched = false;

		private ModuleIdAndVersionStreamIterator() {
			// init _iter
			fetchBatch();
		}

		/**
		 * find next item if necessary
		 */
		private void findNext() {
			while (_next == null && _iter != null) {

				if (!_iter.hasNext()) {
					// end of batch
					fetchBatch();
				}

				if (_iter.hasNext()) {
					_next = _iter.next();
					if (_mapping.isFiltered(_next)) {
						_next = null;
					}
				} else if (_lastBatchFetched) {
					// end reached
					_iter = null;
					break;
				}
			}
		}

		/**
		 * fetch next batch from session if necessary
		 */
		private void fetchBatch() {
			verifyOpen();
			if (_iter == null || !_lastBatchFetched && !_iter.hasNext()) {
				// defer calling as long as possible
				final List<?> list = _rebuildSession.getNext(_batchSize);
				if (list.size() < _batchSize) {
					_lastBatchFetched = true;
				}
				_iter = list.iterator();
			}
		}

		@Override
		public boolean hasNext() {
			findNext();
			return _next != null;
		}

		@Override
		public IdAndVersion next() {
			findNext();

			if (_next == null) {
				throw new NoSuchElementException();
			}

			final Object next = _next;
			_next = null;

			Object id = _mapping.getId(next);
			Long version = _mapping.getVersion(next);

			if (version == null) {
				// force update
				version = Long.MAX_VALUE;
			}

			if (!ObjectIdAndVersion.isIdSupported(id)) {
				id = _mapping.toIdString(id);
			}

			return new ObjectIdAndVersion(id, version, next);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	private void verifyOpen() {
		if (_rebuildSession == null) {
			throw new IllegalStateException("stream not open");
		}
	}

}
