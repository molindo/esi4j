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

import java.io.IOException;
import java.io.ObjectInputStream;

import com.aconex.scrutineer.IdAndVersion;
import com.aconex.scrutineer.IdAndVersionFactory;

import at.molindo.esi4j.mapping.TypeMapping;

public class MappedObjectIdAndVersionFactory implements IdAndVersionFactory {

	private final TypeMapping _mapping;
	private final boolean _convertIds;

	public MappedObjectIdAndVersionFactory(final TypeMapping mapping) {
		if (mapping == null) {
			throw new NullPointerException("mapping");
		}
		_mapping = mapping;
		_convertIds = ObjectIdAndVersion.isIdSupported(mapping.getIdClass());
	}

	@Override
	public IdAndVersion create(Object id, final long version) {
		if (_convertIds && id instanceof String) {
			id = _mapping.toId((String) id);
		}
		return new ObjectIdAndVersion(id, version);
	}

	@Override
	public IdAndVersion readFromStream(final ObjectInputStream inputStream) throws IOException {
		final boolean isString = inputStream.readBoolean();
		Object id;
		if (isString) {
			id = inputStream.readUTF();
		} else {
			id = inputStream.readLong();
		}
		final long version = inputStream.readLong();
		return new ObjectIdAndVersion(id, version);
	}

}
