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

import java.util.List;

import at.molindo.esi4j.rebuild.Esi4JRebuildSession;

public class MetadataRebuildSessionWrapper implements Esi4JRebuildSession {

	private final Esi4JRebuildSession _wrapped;
	private final Object _metadata;

	public MetadataRebuildSessionWrapper(final Esi4JRebuildSession wrapped, final Object metadata) {
		if (wrapped == null) {
			throw new NullPointerException("wrapped");
		}
		_wrapped = wrapped;
		_metadata = metadata;
	}

	@Override
	public boolean isOrdered() {
		return _wrapped.isOrdered();
	}

	@Override
	public Class<?> getType() {
		return _wrapped.getType();
	}

	@Override
	public List<?> getNext(final int batchSize) {
		return _wrapped.getNext(batchSize);
	}

	@Override
	public void close() {
		_wrapped.close();
	}

	@Override
	public Object getMetadata() {
		return _metadata;
	}

}
