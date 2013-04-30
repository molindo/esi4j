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
package at.molindo.esi4j.mapping.impl;

import java.util.Map;

import javax.annotation.Nonnull;

/**
 * basic extension of {@link GenericTypeMapping} that adds setters for ID and
 * version and a read method implementation that takes care of ID and version
 * setting
 */
public abstract class AbstractTypeMapping<Type, Id> extends GenericTypeMapping<Type, Id> {

	public AbstractTypeMapping(String typeAlias, Class<Type> typeClass, Class<Id> idClass) {
		super(typeAlias, typeClass, idClass);
	}

	/**
	 * only public for testing.
	 */
	@Override
	public final Type read(Map<String, Object> source) {
		Type object = readObject(source);

		if (object != null) {

			String id = (String) source.get(FIELD_ID);
			if (id != null) {
				setId(object, toId(id));
			}

			Long version = (Long) source.get(FIELD_VERSION);
			if (version != null && version != -1) {
				setVersion(object, version);
			}
		}

		return object;
	}

	/**
	 * sets id on object after converting elasticsearch id to suitable type
	 * 
	 * @see #toId(String)
	 * @see #setId(Object, Object)
	 */
	public final Id setIdString(@Nonnull Type o, @Nonnull String idString) {
		Id id = toId(idString);
		setId(o, id);
		return id;
	}

	protected abstract void setId(Type o, Id id);

	protected abstract void setVersion(Type o, Long version);

	protected abstract Type readObject(Map<String, Object> source);
}
