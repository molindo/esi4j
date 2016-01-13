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
package at.molindo.esi4j.mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import at.molindo.utils.data.StringUtils;

/**
 * container for {@link TypeMapping}s
 */
public class TypeMappings {

	/**
	 * maps mappings by mapped type
	 */
	private final ConcurrentHashMap<Class<?>, TypeMapping> _mappingsByClass = new ConcurrentHashMap<Class<?>, TypeMapping>();

	/**
	 * maps mappings by type alias
	 */
	private final ConcurrentHashMap<String, TypeMapping> _mappingsByAlias = new ConcurrentHashMap<String, TypeMapping>();

	/**
	 * add a mapping for this {@link TypeMapping}'s type and alias
	 */
	public void addMapping(final TypeMapping mapping) {
		final String typeAlias = mapping.getTypeAlias();
		if (StringUtils.empty(typeAlias)) {
			throw new IllegalArgumentException("typeAlias must not be empty");
		}

		{
			// put alias if unknown
			final TypeMapping prev = _mappingsByAlias.putIfAbsent(typeAlias, mapping);
			if (prev != null && prev != mapping) {
				throw new IllegalArgumentException("duplicate type alias " + mapping.getTypeAlias());
			}
		}

		{
			// put class if unknown
			final TypeMapping prev = _mappingsByClass.putIfAbsent(mapping.getTypeClass(), mapping);
			if (prev != null && prev != mapping) {
				_mappingsByAlias.remove(typeAlias);
				throw new IllegalArgumentException("duplicate type class " + mapping.getTypeClass().getName());
			}
		}
	}

	/**
	 * @return {@link TypeMapping} for this alias or null
	 */
	public TypeMapping getTypeMapping(final String typeAlias) {
		if (StringUtils.empty(typeAlias)) {
			return null;
		}
		return _mappingsByAlias.get(typeAlias);
	}

	/**
	 * @return {@link TypeMapping} for this object or null
	 */
	public TypeMapping getTypeMapping(final Object o) {
		return getTypeMapping(toType(o));
	}

	/**
	 * @return {@link TypeMapping} for this type, one of its superclasses or null
	 */
	public TypeMapping getTypeMapping(Class<?> type) {
		if (type == null) {
			return null;
		}

		TypeMapping mapping = _mappingsByClass.get(type);

		while (mapping == null && (type = type.getSuperclass()) != null) {
			// try superclasses
			mapping = _mappingsByClass.get(type);
		}

		return mapping;
	}

	/**
	 * @return same as {@link #getTypeMapping(String)} but never null
	 * @throws IllegalArgumentException
	 *             if type is not mapped
	 */
	public TypeMapping findTypeMapping(final String typeAlias) {
		final TypeMapping mapping = getTypeMapping(typeAlias);
		if (mapping == null) {
			throw new IllegalArgumentException("unknown type " + typeAlias);
		}
		return mapping;
	}

	/**
	 * @return same as {@link #getTypeMapping(Object)} but never null
	 * @throws IllegalArgumentException
	 *             if object's type is not mapped
	 */
	public TypeMapping findTypeMapping(final Object o) {
		return findTypeMapping(toType(o));
	}

	/**
	 * @return same as {@link #getTypeMapping(Class)} but never null
	 * @throws IllegalArgumentException
	 *             if type is not mapped
	 */
	public TypeMapping findTypeMapping(final Class<?> type) {
		final TypeMapping mapping = getTypeMapping(type);
		if (mapping == null) {
			throw new IllegalArgumentException("unknown class " + type.getName());
		}
		return mapping;
	}

	private Class<? extends Object> toType(final Object o) {
		return o == null ? null : o.getClass();
	}

	public Iterable<TypeMapping> getTypeMappings() {
		return Collections.unmodifiableCollection(_mappingsByAlias.values());
	}

	public Class<?>[] getMappedTypes() {
		// not particularly pretty but thread safe
		final ArrayList<Class<?>> list = new ArrayList<Class<?>>(_mappingsByClass.keySet());
		return list.toArray(new Class<?>[list.size()]);
	}

	public String[] getMappedAliases() {
		// not particularly pretty but thread safe
		final ArrayList<String> list = new ArrayList<String>(_mappingsByAlias.keySet());
		return list.toArray(new String[list.size()]);
	}
}
