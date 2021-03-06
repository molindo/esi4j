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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;

import at.molindo.utils.data.StringUtils;

/**
 * base class for mappings from type to elasticsearch representation
 */
public abstract class TypeMapping {

	public static final String FIELD_INDEX = "_index";
	public static final String FIELD_TYPE = "_type";
	public static final String FIELD_ID = "_id";
	public static final String FIELD_VERSION = "_version";

	private final String _typeAlias;

	/**
	 * creates a {@link TypeMapping} for given alias and class. Both, alias and class must be unique per index.
	 */
	public TypeMapping(final String typeAlias) {
		if (StringUtils.empty(typeAlias)) {
			throw new IllegalArgumentException("typeAlias must not be empty");
		}
		_typeAlias = typeAlias;
	}

	/**
	 * @return the mapping's alias which translates directly to an elasticsearch type
	 */
	public String getTypeAlias() {
		return _typeAlias;
	}

	public abstract Class<?> getTypeClass();

	public abstract Class<?> getIdClass();

	/**
	 * Simple filtering of entities. esi4j core library will always respect this filtering when working with mapped
	 * objects. Users are still free to persist entities anyway.
	 *
	 * @return <code>true</code> if entity must not be persisted to index
	 */
	public boolean isFiltered(@Nonnull final Object o) {
		return false;
	}

	/**
	 * @return <code>true</code> if all object's of mapped type are supposed to be versioned. Note that it isn't
	 *         required to call this method before {@link #getVersion(Object)}
	 */
	public abstract boolean isVersioned();

	/**
	 * @return the object's id
	 * @see #setId(Object, Object)
	 */
	public abstract Object getId(@Nonnull Object o);

	/**
	 * @return the object's id as a string suitable for elasticsearch
	 * @see #getId(Object)
	 * @see #toIdString(Object)
	 */
	public final String getIdString(@Nonnull final Object o) {
		final Object id = getId(o);
		return id == null ? null : toIdString(id);
	}

	/**
	 * @return convert id to a string representation suitable for elasticsearch. never null if id is not null
	 * @see #toId(String)
	 */
	public abstract String toIdString(@Nonnull Object id);

	/**
	 * @return convert string representation suitable for elasticsearch to id
	 * @see #toIdString(Object)
	 */
	public abstract Object toId(@Nonnull String id);

	/**
	 * gets version from object or null if not versioned
	 */
	public abstract Long getVersion(Object o);

	/**
	 * @return a new {@link MappingSource} for this type
	 */
	public abstract MappingSource getMappingSource(Settings indexSettings);

	/**
	 * @return a new {@link ObjectWriteSource} for this object
	 */
	public abstract ObjectWriteSource getObjectSource(Object o);

	/**
	 * @return the object returned by a {@link ObjectReadSource} or null if not applicable or doc doesn't exist
	 */
	@CheckForNull
	public abstract Object read(ObjectReadSource source);

	/**
	 * @return null if object is filtered
	 */
	@CheckForNull
	public final IndexRequestBuilder indexRequest(final Client client, final String indexName, final Object o) {
		return populate(client.prepareIndex(), indexName, o);
	}

	/**
	 * @return null if object is filtered
	 */
	@CheckForNull
	public final IndexRequest indexBuilderRequest(final Client client, final String indexName, final Object o) {
		final IndexRequestBuilder builder = populate(new IndexRequestBuilder(client), indexName, o);
		return builder == null ? null : builder.request();
	}

	private IndexRequestBuilder populate(final IndexRequestBuilder builder, final String indexName, @Nullable final Object o) {
		if (o != null && !isFiltered(o)) {
			builder.setIndex(indexName).setType(getTypeAlias()).setId(getIdString(o));

			final Long version = getVersion(o);
			if (version != null) {
				builder.setVersion(version).setVersionType(VersionType.EXTERNAL);
			}

			getObjectSource(o).setSource(builder);

			return builder;
		} else {
			return null;
		}
	}

	/**
	 * @return null if object doesn't have an id
	 */
	@CheckForNull
	public final DeleteRequest deleteBuilderRequest(final Client client, final String indexName, final Object o) {
		return deleteBuilderRequest(client, indexName, getIdString(o), getVersion(o));
	}

	/**
	 * @return null if object doesn't have an id
	 */
	@CheckForNull
	public final DeleteRequest deleteBuilderRequest(final Client client, final String indexName, final String id, final Long version) {
		final DeleteRequestBuilder builder = populate(new DeleteRequestBuilder(client), indexName, id, version);
		return builder == null ? null : builder.request();
	}

	/**
	 * @return null if object doesn't have an id
	 */
	@CheckForNull
	public final DeleteRequestBuilder deleteRequest(final Client client, final String indexName, final Object o) {
		return deleteRequest(client, indexName, getIdString(o), getVersion(o));
	}

	/**
	 * @return null if object doesn't have an id
	 */
	@CheckForNull
	public final DeleteRequestBuilder deleteRequest(final Client client, final String indexName, final String id, final Long version) {
		return populate(client.prepareDelete(), indexName, id, version);
	}

	private final DeleteRequestBuilder populate(final DeleteRequestBuilder builder, final String indexName, final String id, final Long version) {
		if (id == null) {
			return null;
		}
		builder.setIndex(indexName).setType(getTypeAlias()).setId(id);
		if (version != null) {
			// FIXME `version + 1` see elasticsearch#2946
			builder.setVersion(version + 1).setVersionType(VersionType.EXTERNAL);
		}
		return builder;
	}
}
