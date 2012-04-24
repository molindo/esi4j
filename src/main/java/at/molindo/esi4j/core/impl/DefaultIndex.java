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
package at.molindo.esi4j.core.impl;

import javax.annotation.Nonnull;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import at.molindo.esi4j.action.BulkResponseWrapper;
import at.molindo.esi4j.action.CountResponseWrapper;
import at.molindo.esi4j.action.DeleteResponseWrapper;
import at.molindo.esi4j.action.GetResponseWrapper;
import at.molindo.esi4j.action.IndexResponseWrapper;
import at.molindo.esi4j.action.SearchResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultBulkResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultCountResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultDeleteResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultGetResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultIndexResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultSearchResponseWrapper;
import at.molindo.esi4j.core.Esi4JIndexManager;
import at.molindo.esi4j.core.Esi4JStore;
import at.molindo.esi4j.core.Esi4JStore.StoreOperation;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.mapping.TypeMappings;
import at.molindo.esi4j.util.ListenableActionFutureWrapper;
import at.molindo.utils.data.Function;

public class DefaultIndex implements InternalIndex {

	private final String _name;
	private final Settings _settings;

	private final TypeMappings _mappings;

	private Esi4JIndexManager _indexManager;

	@Nonnull
	private Esi4JStore _store;

	public DefaultIndex(String name, Settings settings, Esi4JStore store) {
		if (name == null) {
			throw new NullPointerException("id");
		}
		_name = name;
		_settings = settings;

		_mappings = new TypeMappings();
		setStore(store);
	}

	public String getName() {
		return _name;
	}

	@Override
	public Settings getSettings() {
		return _settings;
	}

	@Override
	public Esi4JStore getStore() {
		return _store;
	}

	public void setStore(Esi4JStore store) {
		if (store == null) {
			throw new NullPointerException("store");
		}
		store.setIndex(this);
		_store = store;

		for (TypeMapping typeMapping : _mappings.getTypeMappings()) {
			putMapping(typeMapping);
		}
	}

	@Override
	public void close() {
		if (_indexManager != null) {
			_indexManager.close();
		}
		_store.close();
	}

	public DefaultIndex addTypeMapping(TypeMapping typeMapping) {
		_mappings.addMapping(typeMapping);
		putMapping(typeMapping);
		return this;
	}

	@Override
	public void updateMapping(Class<?> type) {
		putMapping(_mappings.findTypeMapping(type));
	}

	@Override
	public void updateMapping(String typeAlias) {
		putMapping(_mappings.findTypeMapping(typeAlias));
	}

	@Override
	public Class<?>[] getMappedTypes() {
		return _mappings.getMappedTypes();
	}

	protected void putMapping(final TypeMapping typeMapping) {
		ListenableActionFuture<PutMappingResponse> future = _store
				.execute(new StoreOperation<ListenableActionFuture<PutMappingResponse>>() {

					@Override
					public ListenableActionFuture<PutMappingResponse> execute(Client client, String indexName) {
						PutMappingRequestBuilder request = client.admin().indices().preparePutMapping(indexName);
						request.setType(typeMapping.getTypeAlias());
						typeMapping.getMappingSource().setSource(request);
						return request.execute();
					}

				});

		// TODO handle response
		future.actionGet();
	}

	@Override
	public <T> T execute(final IndexOperation<T> operation) {
		return _store.execute(new StoreOperation<T>() {

			@Override
			public T execute(Client client, String indexName) {
				return operation.execute(client, indexName, new Helper());
			}
		});
	}

	@Override
	public ListenableActionFuture<IndexResponseWrapper> index(final Object o) {
		return executeIndex(new Index(o));
	}

	@Override
	public ListenableActionFuture<IndexResponseWrapper> executeIndex(
			IndexOperation<ListenableActionFuture<IndexResponse>> indexOperation) {

		return ListenableActionFutureWrapper.wrap(execute(indexOperation),
				new Function<IndexResponse, IndexResponseWrapper>() {

					@Override
					public IndexResponseWrapper apply(IndexResponse input) {
						TypeMapping typeMapping = _mappings.getTypeMapping(input.getType());
						Object id = typeMapping.toId(input.getId());
						return new DefaultIndexResponseWrapper(input, id);
					}
				});
	}

	@Override
	public ListenableActionFuture<GetResponseWrapper> get(Class<?> type, Object id) {
		return executeGet(new Get(type, id));
	}

	@Override
	public ListenableActionFuture<GetResponseWrapper> executeGet(
			IndexOperation<ListenableActionFuture<GetResponse>> getOperation) {

		return ListenableActionFutureWrapper.wrap(execute(getOperation),
				new Function<GetResponse, GetResponseWrapper>() {

					@Override
					public GetResponseWrapper apply(GetResponse input) {
						TypeMapping typeMapping = _mappings.getTypeMapping(input.getType());
						Object object = typeMapping.read(input);
						return new DefaultGetResponseWrapper(input, object);
					}
				});
	}

	public ListenableActionFuture<DeleteResponseWrapper> executeDelete(
			IndexOperation<ListenableActionFuture<DeleteResponse>> deleteOperation) {
		return ListenableActionFutureWrapper.wrap(execute(deleteOperation),
				new Function<DeleteResponse, DeleteResponseWrapper>() {

					@Override
					public DeleteResponseWrapper apply(DeleteResponse input) {
						return new DefaultDeleteResponseWrapper(input);
					}
				});
	}

	@Override
	public ListenableActionFuture<DeleteResponseWrapper> delete(Object object) {
		TypeMapping typeMapping = _mappings.findTypeMapping(object);
		return executeDelete(new Delete(typeMapping.getTypeClass(), typeMapping.getId(object)));
	}

	@Override
	public ListenableActionFuture<DeleteResponseWrapper> delete(Class<?> type, Object id) {
		return executeDelete(new Delete(type, id));
	}

	@Override
	public ListenableActionFuture<SearchResponseWrapper> search(QueryBuilder query, Class<?> type) {
		return executeSearch(new Search(query, type));
	}

	@Override
	public ListenableActionFuture<SearchResponseWrapper> search(QueryBuilder query, Class<?> type, int from, int size) {
		return executeSearch(new Search(query, type, from, size));
	}

	@Override
	public ListenableActionFuture<SearchResponseWrapper> executeSearch(
			IndexOperation<ListenableActionFuture<SearchResponse>> searchOperation) {
		return ListenableActionFutureWrapper.wrap(execute(searchOperation),
				new Function<SearchResponse, SearchResponseWrapper>() {

					@Override
					public SearchResponseWrapper apply(SearchResponse response) {
						return new DefaultSearchResponseWrapper(response, DefaultIndex.this);
					}
				});
	}

	@Override
	public ListenableActionFuture<CountResponseWrapper> count(QueryBuilder query, Class<?> type) {
		return executeCount(new Count(query, type));
	}

	@Override
	public ListenableActionFuture<CountResponseWrapper> executeCount(
			IndexOperation<ListenableActionFuture<CountResponse>> countOperation) {

		return ListenableActionFutureWrapper.wrap(execute(countOperation),
				new Function<CountResponse, CountResponseWrapper>() {

					@Override
					public CountResponseWrapper apply(CountResponse response) {
						return new DefaultCountResponseWrapper(response);
					}
				});
	}

	@Override
	public ListenableActionFuture<BulkResponseWrapper> bulkIndex(final Iterable<?> iterable) {
		return executeBulk(new IndexOperation<ListenableActionFuture<BulkResponse>>() {

			@Override
			public ListenableActionFuture<BulkResponse> execute(Client client, String indexName, OperationHelper helper) {
				BulkRequestBuilder request = client.prepareBulk();

				for (Object o : iterable) {
					TypeMapping mapping = helper.findTypeMapping(o);
					String id = mapping.getIdString(o);

					IndexRequestBuilder index = client.prepareIndex(indexName, mapping.getTypeAlias());
					if (id != null) {
						index.setId(id);
					}
					mapping.getObjectSource(o).setSource(index);

					request.add(index);
				}

				return request.execute();
			}
		});
	}

	public ListenableActionFuture<BulkResponseWrapper> executeBulk(
			IndexOperation<ListenableActionFuture<BulkResponse>> bulkOperation) {
		return ListenableActionFutureWrapper.wrap(execute(bulkOperation),
				new Function<BulkResponse, BulkResponseWrapper>() {

					@Override
					public BulkResponseWrapper apply(BulkResponse response) {
						return new DefaultBulkResponseWrapper(response);
					}
				});
	}

	@Override
	public void refresh() {
		ListenableActionFuture<RefreshResponse> future = _store
				.execute(new StoreOperation<ListenableActionFuture<RefreshResponse>>() {

					@Override
					public ListenableActionFuture<RefreshResponse> execute(Client client, String indexName) {
						return client.admin().indices().prepareRefresh(indexName).execute();
					}

				});

		// TODO handle response
		future.actionGet();
	}

	@Override
	public Object read(SearchHit hit) {
		return _mappings.findTypeMapping(hit.getType()).read(hit);
	}

	public Esi4JIndexManager getIndexManager() {
		return _indexManager;
	}

	public void setIndexManager(Esi4JIndexManager indexManager) {
		if (_indexManager != null) {
			throw new IllegalStateException("indexManager already assigned");
		}

		_indexManager = indexManager;
	}

	private static final class Index implements IndexOperation<ListenableActionFuture<IndexResponse>> {
		private final Object _object;

		private Index(Object object) {
			if (object == null) {
				throw new NullPointerException("object");
			}
			_object = object;
		}

		@Override
		public ListenableActionFuture<IndexResponse> execute(Client client, String indexName, OperationHelper helper) {
			final TypeMapping typeMapping = helper.findTypeMapping(_object);

			final String type = typeMapping.getTypeAlias();
			final String id = typeMapping.getIdString(_object);

			IndexRequestBuilder request = client.prepareIndex(indexName, type, id);
			typeMapping.getObjectSource(_object).setSource(request);
			return request.execute();
		}
	}

	private static final class Get implements IndexOperation<ListenableActionFuture<GetResponse>> {

		private final Class<?> _type;
		private final Object _id;

		private Get(Class<?> type, Object id) {
			if (type == null) {
				throw new NullPointerException("type");
			}
			if (id == null) {
				throw new NullPointerException("id");
			}
			_type = type;
			_id = id;
		}

		@Override
		public ListenableActionFuture<GetResponse> execute(Client client, String indexName, OperationHelper helper) {
			final TypeMapping typeMapping = helper.findTypeMapping(_type);

			final String type = typeMapping.getTypeAlias();
			final String id = typeMapping.toIdString(_id);

			return client.prepareGet(indexName, type, id).execute();
		}
	}

	private static final class Delete implements IndexOperation<ListenableActionFuture<DeleteResponse>> {

		private final Class<?> _type;
		private final Object _id;

		private Delete(Class<?> type, Object id) {
			if (type == null) {
				throw new NullPointerException("type");
			}
			if (id == null) {
				throw new NullPointerException("id");
			}
			_type = type;
			_id = id;
		}

		@Override
		public ListenableActionFuture<DeleteResponse> execute(Client client, String indexName, OperationHelper helper) {
			final TypeMapping typeMapping = helper.findTypeMapping(_type);

			final String type = typeMapping.getTypeAlias();
			final String id = typeMapping.toIdString(_id);

			return client.prepareDelete(indexName, type, id).execute();

		}
	}

	private static final class Search implements IndexOperation<ListenableActionFuture<SearchResponse>> {

		private QueryBuilder _query;
		private final Class<?> _type;

		private final int _from;
		private final int _size;

		private Search(QueryBuilder query, Class<?> type) {
			this(query, type, 0, Integer.MAX_VALUE);
		}

		private Search(QueryBuilder query, Class<?> type, int from, int size) {
			if (type == null) {
				throw new NullPointerException("type");
			}
			if (query == null) {
				throw new NullPointerException("query");
			}
			_query = query;
			_type = type;
			_from = from;
			_size = size;
		}

		@Override
		public ListenableActionFuture<SearchResponse> execute(Client client, String indexName, OperationHelper helper) {
			final TypeMapping typeMapping = helper.findTypeMapping(_type);

			final String type = typeMapping.getTypeAlias();

			SearchRequestBuilder builder = client.prepareSearch(indexName).setTypes(type).setQuery(_query)
					.setFrom(_from).setSize(_size);

			return builder.execute();
		}

	}

	private static final class Count implements IndexOperation<ListenableActionFuture<CountResponse>> {

		private QueryBuilder _query;
		private final Class<?> _type;

		private Count(QueryBuilder query, Class<?> type) {
			if (type == null) {
				throw new NullPointerException("type");
			}
			if (query == null) {
				throw new NullPointerException("query");
			}
			_query = query;
			_type = type;
		}

		@Override
		public ListenableActionFuture<CountResponse> execute(Client client, String indexName, OperationHelper helper) {
			final TypeMapping typeMapping = helper.findTypeMapping(_type);

			final String type = typeMapping.getTypeAlias();

			CountRequestBuilder builder = client.prepareCount(indexName).setTypes(type).setQuery(_query);

			return builder.execute();
		}

	}

	private final class Helper implements OperationHelper {

		@Override
		public TypeMapping findTypeMapping(Object o) {
			return _mappings.findTypeMapping(o);
		}

		@Override
		public TypeMapping findTypeMapping(Class<?> type) {
			return _mappings.findTypeMapping(type);
		}
	}

}
