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
package at.molindo.esi4j.chain;

import java.io.Serializable;

import at.molindo.esi4j.chain.impl.QueuedTaskExecutor;

/**
 * replaces objects with placeholders (e.g. for serialization) and resolves them
 * for indexing. For deletions, an
 * 
 * @see QueuedTaskExecutor
 */
public interface Esi4JEntityResolver {

	/**
	 * @return a new {@link ObjectKey} for this entity
	 */
	ObjectKey toObjectKey(Object entity);

	/**
	 * @return a resolveable placeholder for this entity. Might be a new
	 *         {@link ObjectKey}
	 */
	Object replaceEntity(Object entity);

	/**
	 * @return the replaced entity, the given entity if not replaced or
	 *         <code>null</code> if entity cannot be resolved
	 */
	Object resolveEntity(Object replacedEntity);

	/**
	 * an identifier consisting of a type, a {@link Serializable} id and
	 * optionally a version
	 */
	public static final class ObjectKey implements Serializable {

		private static final long serialVersionUID = 1L;

		private final Class<?> _type;
		private final Serializable _id;
		private final Long _version;

		public ObjectKey(Class<?> type, Serializable id) {
			this(type, id, null);
		}

		public ObjectKey(Class<?> type, Serializable id, Long version) {
			if (type == null) {
				throw new NullPointerException("type");
			}
			if (id == null) {
				throw new NullPointerException("id");
			}
			_type = type;
			_id = id;
			_version = version;
		}

		/**
		 * @return the object type
		 */
		public Class<?> getType() {
			return _type;
		}

		/**
		 * @return never null
		 */
		public Serializable getId() {
			return _id;
		}

		/**
		 * @return the object version or null if not available
		 */
		public Long getVersion() {
			return _version;
		}

		@Override
		public String toString() {
			StringBuilder buf = new StringBuilder(_type.getSimpleName()).append("#").append(_id);
			if (_version != null) {
				buf.append(" (").append(_version).append(")");
			}
			return buf.toString();
		}

	}
}
