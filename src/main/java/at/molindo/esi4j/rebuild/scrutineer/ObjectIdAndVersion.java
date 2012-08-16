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
import java.io.ObjectOutputStream;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.aconex.scrutineer.AbstractIdAndVersion;
import com.aconex.scrutineer.IdAndVersion;

public class ObjectIdAndVersion extends AbstractIdAndVersion {

	private final Object _object;
	private final Object _id;

	public static boolean isIdSupported(Object id) {
		return id instanceof Number || id instanceof String;
	}

	public ObjectIdAndVersion(Object id, long version) {
		this(id, version, null);
	}

	public ObjectIdAndVersion(Object id, long version, @Nullable Object object) {
		super(version);
		if (id instanceof Number) {
			id = ((Number) id).longValue();
		}
		if (id instanceof Long || id instanceof String) {
			_object = object;
			_id = id;
		} else {
			throw new IllegalArgumentException("unexpected id: " + id);
		}
	}

	@CheckForNull
	public Object getObject() {
		return _object;
	}

	@Override
	public String getId() {
		return _id.toString();
	}

	@Override
	protected HashCodeBuilder appendId(HashCodeBuilder appender) {
		return appender.append(_id);
	}

	@Override
	protected CompareToBuilder appendId(CompareToBuilder appender, IdAndVersion other) {
		return appender.append(_id, ((ObjectIdAndVersion) other)._id);
	}

	@Override
	protected void writeId(ObjectOutputStream objectOutputStream) throws IOException {
		if (_id instanceof String) {
			objectOutputStream.writeBoolean(true);
			objectOutputStream.writeUTF((String) _id);
		} else {
			objectOutputStream.writeBoolean(false);
			objectOutputStream.writeLong(((Number) _id).longValue());
		}
	}

}