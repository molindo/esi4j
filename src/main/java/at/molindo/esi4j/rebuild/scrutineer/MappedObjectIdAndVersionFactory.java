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
import java.io.ObjectOutputStream;
import java.util.Map;

import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;

import at.molindo.esi4j.mapping.ObjectReadSource;
import at.molindo.esi4j.mapping.ObjectWriteSource;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.scrutineer.IdAndVersion;
import at.molindo.scrutineer.IdAndVersionFactory;

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
	public ObjectIdAndVersion readFromStream(final ObjectInputStream inputStream) throws IOException {
		final boolean isString = inputStream.readBoolean();
		Object id;
		if (isString) {
			id = inputStream.readUTF();
		} else {
			id = inputStream.readLong();
		}
		final long version = inputStream.readLong();

		final int length = inputStream.readInt();
		if (length == 0) {
			return new ObjectIdAndVersion(id, version);
		} else {
			final byte[] bytes = new byte[length];
			inputStream.readFully(bytes);

			final Map<String, Object> map = Requests.INDEX_CONTENT_TYPE.xContent().createParser(bytes).map();
			final Object object = _mapping
					.read(ObjectReadSource.Builder.map(ObjectIdAndVersion.toId(id), version, map));
			return new ObjectIdAndVersion(id, version, object);
		}

	}

	@Override
	public void writeToStream(final IdAndVersion idAndVersion, final ObjectOutputStream objectOutputStream) throws IOException {
		// write id - boolean flag to indicate string
		final Object id = ((ObjectIdAndVersion) idAndVersion).getRawId();
		if (id instanceof String) {
			objectOutputStream.writeBoolean(true);
			objectOutputStream.writeUTF((String) id);
		} else {
			objectOutputStream.writeBoolean(false);
			objectOutputStream.writeLong(((Number) id).longValue());
		}

		// write version
		objectOutputStream.writeLong(idAndVersion.getVersion());

		// write object - start with byte array length
		final Object object = ((ObjectIdAndVersion) idAndVersion).getObject();
		if (object == null) {
			objectOutputStream.writeInt(0);
		} else {
			final ObjectWriteSource src = _mapping.getObjectSource(object);
			final BytesReference bytes = src.getSource();
			if (bytes == null || bytes.length() == 0) {
				objectOutputStream.writeInt(0);
			} else {
				objectOutputStream.writeInt(bytes.length());
				objectOutputStream.write(bytes.array(), bytes.arrayOffset(), bytes.length());
			}
		}
	}

}
