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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper.Builder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import at.molindo.esi4j.mapping.MappingSource;
import at.molindo.esi4j.mapping.ObjectSource;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.utils.collections.CollectionUtils;

import com.google.common.collect.Maps;

/**
 * generic version of {@link TypeMapping}. All subclasses should extend this
 * class instead of {@link TypeMapping} while it's generally better to use
 * {@link TypeMapping} where the exact type of mapping is not known or not
 * relevant
 * 
 * @param <Type>
 *            class of mapped type
 * @param <Id>
 *            class of mapped type's ID
 */
public abstract class GenericTypeMapping<Type, Id> extends TypeMapping {

	private final Class<Type> _typeClass;
	private final Class<Id> _idClass;

	private String _mapping;

	public GenericTypeMapping(String typeAlias, Class<Type> typeClass, Class<Id> idClass) {
		super(typeAlias);
		if (typeClass == null) {
			throw new NullPointerException("typeClass");
		}
		if (idClass == null) {
			throw new NullPointerException("idClass");
		}
		_typeClass = typeClass;
		_idClass = idClass;
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
	public ObjectSource getObjectSource(Object o) {
		try {
			return ObjectSource.Builder.builder(getContentBuilder(o));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * public for testing.
	 * 
	 * @return a JsonXContent builder
	 */
	public final XContentBuilder getContentBuilder(Object o) throws IOException {
		XContentBuilder contentBuilder = JsonXContent.contentBuilder();
		write(contentBuilder, o);
		return contentBuilder;
	}

	public final void write(XContentBuilder contentBuilder, Object o) throws IOException {
		contentBuilder.startObject();

		// TODO why do we add the id to the document source?
		String id = getIdString(o);
		if (id != null) {
			// skip empty id for id generation
			contentBuilder.field(FIELD_ID).value(id);
		}

		writeObject(contentBuilder, cast(o));
		contentBuilder.endObject();
	}

	@Override
	public final Type read(GetResponse response) {
		return response.isExists() ? read(getSource(response)) : null;
	}

	@Override
	public final Type read(SearchHit hit) {
		return read(getSource(hit));
	}

	/**
	 * @return map containing all properties of a {@link GetResponse}
	 * @see #getSource(SearchHit)
	 */
	protected Map<String, Object> getSource(GetResponse response) {
		Map<String, Object> map = response.getSource();
		if (map == null) {
			map = Maps.newHashMap();
			for (Entry<String, GetField> e : response.getFields().entrySet()) {
				List<?> values = e.getValue().getValues();
				if (!CollectionUtils.empty(values)) {
					map.put(e.getKey(), values.size() == 1 ? values.get(0) : values);
				}
			}
		}
		map.put(FIELD_INDEX, response.getIndex());
		map.put(FIELD_TYPE, response.getType());
		map.put(FIELD_ID, response.getId());
		if (response.getVersion() != -1) {
			map.put(FIELD_VERSION, response.getVersion());
		}
		return map;
	}

	/**
	 * @return map containing all properties of a {@link SearchHit}
	 * @see #getSource(GetResponse)
	 */
	protected Map<String, Object> getSource(SearchHit hit) {
		Map<String, Object> map = hit.sourceAsMap();
		if (map == null) {
			map = Maps.newHashMap();
			for (Entry<String, SearchHitField> e : hit.getFields().entrySet()) {
				List<?> values = e.getValue().getValues();
				if (!CollectionUtils.empty(values)) {
					map.put(e.getKey(), values.size() == 1 ? values.get(0) : values);
				}
			}
		}
		map.put(FIELD_INDEX, hit.getIndex());
		map.put(FIELD_TYPE, hit.getType());
		map.put(FIELD_ID, hit.getId());
		if (hit.getVersion() != -1) {
			map.put(FIELD_VERSION, hit.getVersion());
		}
		return map;
	}

	@Override
	public Class<Type> getTypeClass() {
		return _typeClass;
	}

	@Override
	public Class<Id> getIdClass() {
		return _idClass;
	}

	protected Type cast(Object o) {
		return getTypeClass().cast(o);
	}

	protected Id castId(Object o) {
		return getIdClass().cast(o);
	}

	@Override
	public final boolean isFiltered(Object entity) {
		return filter(cast(entity));
	}

	@Override
	public final Id getId(Object o) {
		return id(cast(o));
	}

	@Override
	public Long getVersion(Object o) {
		return version(cast(o));
	}

	protected boolean filter(Type o) {
		return false;
	}

	@Override
	public final String toIdString(Object id) {
		return toString(castId(id));
	}

	public abstract String toString(Id id);

	@Override
	public abstract Id toId(String id);

	/**
	 * get Id from object
	 * 
	 * @see #getId(Object)
	 */
	protected abstract Id id(Type o);

	/**
	 * get version from object
	 * 
	 * @see #getVersion(Object)
	 */
	protected abstract Long version(Type o);

	protected abstract void buildMapping(RootObjectMapper.Builder mapperBuilder) throws IOException;

	protected abstract void writeObject(XContentBuilder contentBuilder, Type o) throws IOException;

	/**
	 * read object from source. Publicly accessible for testing
	 */
	public abstract Type read(Map<String, Object> source);

}
