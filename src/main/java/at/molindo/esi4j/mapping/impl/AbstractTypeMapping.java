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

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper.Builder;
import org.elasticsearch.search.SearchHit;

import at.molindo.esi4j.mapping.MappingSource;
import at.molindo.esi4j.mapping.ObjectSource;
import at.molindo.esi4j.mapping.TypeMapping;

/**
 * basic implementation of {@link TypeMapping} to simplify mapping
 */
public abstract class AbstractTypeMapping<Type, Id> extends TypeMapping {

	private final Class<Id> _idClass;
	private String _mapping;

	public AbstractTypeMapping(String typeAlias, Class<Type> typeClass, Class<Id> idClass) {
		super(typeAlias, typeClass);
		if (idClass == null) {
			throw new NullPointerException("idClass");
		}
		_idClass = idClass;
	}

	@SuppressWarnings("unchecked")
	protected Type cast(Object o) {
		return (Type) getTypeClass().cast(o);
	}

	protected Id castId(Object o) {
		return _idClass.cast(o);
	}

	@Override
	public final boolean isFiltered(Object entity) {
		return filter(cast(entity));
	}

	@Override
	public final Object getId(Object o) {
		return id(cast(o));
	}

	@Override
	public final void setId(Object o, Object id) {
		id(cast(o), castId(id));
	}

	@Override
	public final String toIdString(Object id) {
		return toString(castId(id));
	}

	@Override
	public final Object toId(String id) {
		return fromString(id);
	}

	@Override
	public final Long getVersion(Object o) {
		return version(cast(o));
	}

	@Override
	public void setVersion(Object o, Long version) {
		version(cast(o), version);
	}

	@Override
	public final Object read(GetResponse response) {
		return read(getSource(response));
	}

	@Override
	public final Object read(SearchHit hit) {
		return read(getSource(hit));
	}

	private Object read(Map<String, Object> source) {
		Type object = readObject(source);
		setIdString(object, (String) source.get(FIELD_ID));
		setVersion(object, (Long) source.get(FIELD_VERSION));
		return object;
	}

	@Override
	public final MappingSource getMappingSource() {
		try {
			if (_mapping == null) {
				Builder mapperBuilder = new RootObjectMapper.Builder(getTypeAlias());

				buildMapping(mapperBuilder);

				XContentBuilder contentBuilder = JsonXContent.contentBuilder();

				contentBuilder.startObject();
				mapperBuilder.build(new BuilderContext(null, new ContentPath())).toXContent(contentBuilder, null);
				contentBuilder.endObject();

				// cache mapping as string for easy debugging
				_mapping = contentBuilder.string();
			}
			return MappingSource.Builder.string(_mapping);
		} catch (IOException e) {
			throw new RuntimeException();
		}
	}

	@Override
	public final ObjectSource getObjectSource(Object o) {
		try {
			XContentBuilder contentBuilder = JsonXContent.contentBuilder();

			contentBuilder.startObject();

			String id = getIdString(o);
			if (id != null) {
				// skip empty id for id generation
				contentBuilder.field(FIELD_ID).value(id);
			}

			writeObject(contentBuilder, cast(o));
			contentBuilder.endObject();

			return ObjectSource.Builder.builder(contentBuilder);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected boolean filter(Type o) {
		return false;
	}

	protected abstract Id id(Type o);

	protected abstract void id(Type o, Id id);

	protected abstract String toString(Id id);

	protected abstract Id fromString(String id);

	protected abstract Long version(Type o);

	protected abstract void version(Type o, Long version);

	protected abstract void buildMapping(RootObjectMapper.Builder mapperBuilder) throws IOException;

	protected abstract void writeObject(XContentBuilder contentBuilder, Type o) throws IOException;

	protected abstract Type readObject(Map<String, Object> source);
}
