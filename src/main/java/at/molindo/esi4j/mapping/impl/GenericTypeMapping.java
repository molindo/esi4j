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

import static org.elasticsearch.common.xcontent.ToXContent.*;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper.Builder;

import com.google.common.collect.ImmutableMap;

import at.molindo.esi4j.mapping.MappingSource;
import at.molindo.esi4j.mapping.ObjectReadSource;
import at.molindo.esi4j.mapping.ObjectWriteSource;
import at.molindo.esi4j.mapping.TypeMapping;

/**
 * generic version of {@link TypeMapping}. All subclasses should extend this class instead of {@link TypeMapping} while
 * it's generally better to use {@link TypeMapping} where the exact type of mapping is not known or not relevant
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

	public GenericTypeMapping(final String typeAlias, final Class<Type> typeClass, final Class<Id> idClass) {
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
	public final MappingSource getMappingSource(final Settings settings) {
		try {
			if (_mapping == null || isDynamicMapping()) {
				final Builder mapperBuilder = new RootObjectMapper.Builder(getTypeAlias());

				buildMapping(mapperBuilder);

				final XContentBuilder contentBuilder = JsonXContent.contentBuilder();

				contentBuilder.startObject();

				mapperBuilder.build(new BuilderContext(settings, new ContentPath()))
						.toXContent(contentBuilder, EMPTY_PARAMS, new ToXContent() {

							@Override
							public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
								final ImmutableMap<String, Object> meta = meta();
								if (meta != null && !meta.isEmpty()) {
									builder.field("_meta", meta);
								}
								return builder;
							}
						});
				contentBuilder.endObject();

				// cache mapping as string for easy debugging
				_mapping = contentBuilder.string();
			}
			return MappingSource.Builder.string(_mapping);
		} catch (final IOException e) {
			throw new RuntimeException();
		}
	}

	/**
	 * @return true if mapping must not be cached
	 */
	public boolean isDynamicMapping() {
		return false;
	}

	public ImmutableMap<String, Object> meta() {
		return null;
	}

	@Override
	public ObjectWriteSource getObjectSource(final Object o) {
		try {
			return ObjectWriteSource.Builder.builder(getContentBuilder(o));
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * public for testing.
	 *
	 * @return a JsonXContent builder
	 */
	public final XContentBuilder getContentBuilder(final Object o) throws IOException {
		final XContentBuilder contentBuilder = JsonXContent.contentBuilder();
		write(contentBuilder, o);
		return contentBuilder;
	}

	public final void write(final XContentBuilder contentBuilder, final Object o) throws IOException {
		contentBuilder.startObject();

		// TODO why do we add the id to the document source?
		final String id = getIdString(o);
		if (id != null) {
			// skip empty id for id generation
			contentBuilder.field(FIELD_ID).value(id);
		}

		writeObject(contentBuilder, cast(o));
		contentBuilder.endObject();
	}

	@Override
	public Class<Type> getTypeClass() {
		return _typeClass;
	}

	@Override
	public Class<Id> getIdClass() {
		return _idClass;
	}

	protected Type cast(final Object o) {
		return getTypeClass().cast(o);
	}

	protected Id castId(final Object o) {
		return getIdClass().cast(o);
	}

	@Override
	public final boolean isFiltered(final Object entity) {
		return filter(cast(entity));
	}

	@Override
	public final Id getId(final Object o) {
		return id(cast(o));
	}

	@Override
	public Long getVersion(final Object o) {
		return version(cast(o));
	}

	protected boolean filter(final Type o) {
		return false;
	}

	@Override
	public final String toIdString(final Object id) {
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

	@Override
	public Object read(final ObjectReadSource source) {
		final Map<String, Object> map = source.map();
		return map == null ? null : read(map);
	}

	/**
	 * read object from source. Publicly accessible for testing
	 */
	public abstract Type read(Map<String, Object> source);

}
