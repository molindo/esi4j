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
import at.molindo.esi4j.module.Esi4JModule;
import at.molindo.esi4j.rebuild.Esi4JRebuildSession;

import com.aconex.scrutineer.IdAndVersion;
import com.aconex.scrutineer.IdAndVersionStream;

public final class ModuleIdAndVersionStream implements IdAndVersionStream {

	private final Esi4JModule _module;
	private final int _batchSize;
	private final TypeMapping _mapping;

	private Esi4JRebuildSession<?> _session;

	public ModuleIdAndVersionStream(Esi4JModule module, int batchSize, TypeMapping mapping) {
		_module = module;
		_batchSize = batchSize;
		_mapping = mapping;
	}

	@Override
	public void open() {
		_session = _module.startRebuildSession(_mapping.getTypeClass());
	}

	@Override
	public Iterator<IdAndVersion> iterator() {
		if (_session == null) {
			throw new IllegalStateException("stream not open");
		}
		return new ModuleIdAndVersionStreamIterator();
	}

	@Override
	public void close() {
		_session.close();
		_session = null;
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
			if (_iter == null || !_lastBatchFetched && !_iter.hasNext()) {
				// defer calling as long as possible
				List<?> list = _session.getNext(_batchSize);
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

			Object next = _next;
			_next = null;

			String id = _mapping.getIdString(next);
			Long version = _mapping.getVersion(next);

			if (version == null) {
				throw new IllegalArgumentException("version required for scrutineer");
			}
			return new ObjectIdAndVersion(next, id, version);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}