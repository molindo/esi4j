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

import javax.annotation.Nullable;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import at.molindo.scrutineer.AbstractIdAndVersion;
import at.molindo.scrutineer.IdAndVersion;

public class ObjectIdAndVersion extends AbstractIdAndVersion {

	private final Object _object;
	private final Comparable<?> _id;

	public static boolean isIdSupported(final Class<?> idClass) {
		return String.class.equals(idClass) || Number.class.isAssignableFrom(idClass);
	}

	public static boolean isIdSupported(final Object id) {
		return id instanceof Number || id instanceof String;
	}

	/**
	 * used for id and version coming from current index
	 *
	 * @see MappedObjectIdAndVersionFactory
	 */
	public ObjectIdAndVersion(final Object id, final long version) {
		this(id, version, null);
	}

	/**
	 * @param id
	 * @param version
	 * @param object
	 *            may be null when coming from index
	 */
	public ObjectIdAndVersion(Object id, final long version, @Nullable final Object object) {
		super(version);
		if (id instanceof Number) {
			id = ((Number) id).longValue();
		}
		if (id instanceof Long || id instanceof String) {
			_object = object;
			_id = (Comparable<?>) id;
		} else {
			throw new IllegalArgumentException("unexpected id: " + id);
		}
	}

	/**
	 * @return <code>null</code> only for currently indexed objects
	 */
	@Nullable
	public Object getObject() {
		return _object;
	}

	@Override
	public String getId() {
		return _id.toString();
	}

	@Override
	protected HashCodeBuilder appendId(final HashCodeBuilder appender) {
		return appender.append(_id);
	}

	@Override
	protected CompareToBuilder appendId(final CompareToBuilder appender, final IdAndVersion other) {
		return appender.append(_id, ((ObjectIdAndVersion) other)._id);
	}

}
