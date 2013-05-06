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

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import at.molindo.esi4j.action.CountResponseWrapper;
import at.molindo.esi4j.action.GetResponseWrapper;
import at.molindo.esi4j.action.MultiGetItemResponseWrapper.MultiGetItemReader;
import at.molindo.esi4j.action.MultiGetResponseWrapper;
import at.molindo.esi4j.action.SearchHitWrapper.SearchHitReader;
import at.molindo.esi4j.action.SearchResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultCountResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultGetResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultMultiGetResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultSearchResponseWrapper;
import at.molindo.esi4j.core.Esi4JManagedIndex;
import at.molindo.esi4j.core.Esi4JOperation;
import at.molindo.esi4j.core.Esi4JOperation.OperationContext;
import at.molindo.esi4j.core.Esi4JSearchIndex;
import at.molindo.esi4j.core.Esi4JStore;
import at.molindo.esi4j.core.Esi4JStore.StoreOperation;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.util.ListenableActionFutureWrapper;
import at.molindo.utils.data.Function;

public abstract class AbstractIndex implements Esi4JSearchIndex, Esi4JManagedIndex, OperationContext, SearchHitReader,
		MultiGetItemReader {

	@Override
	public <T> T execute(final Esi4JOperation<T> operation) {
		return getStore().execute(new StoreOperation<T>() {

			@Override
			public T execute(Client client, String indexName) {
				return operation.execute(client, indexName, AbstractIndex.this);
			}
		});
	}

	@Override
	public ListenableActionFuture<GetResponseWrapper> get(Class<?> type, Object id) {
		return executeGet(new Get(type, id));
	}

	@Override
	public ListenableActionFuture<GetResponseWrapper> executeGet(
			Esi4JOperation<ListenableActionFuture<GetResponse>> getOperation) {

		return ListenableActionFutureWrapper.wrap(execute(getOperation),
				new Function<GetResponse, GetResponseWrapper>() {

					@Override
					public GetResponseWrapper apply(GetResponse input) {
						TypeMapping typeMapping = findTypeMapping(input.getIndex(), input.getType());
						Object object = typeMapping.read(input, AbstractIndex.this);
						return new DefaultGetResponseWrapper(input, object);
					}
				});
	}

	@Override
	public ListenableActionFuture<MultiGetResponseWrapper> multiGet(Class<?> type, Iterable<?> ids) {
		return executeMultiGet(new MultiGet(type, ids));
	}

	@Override
	public ListenableActionFuture<MultiGetResponseWrapper> executeMultiGet(
			Esi4JOperation<ListenableActionFuture<MultiGetResponse>> multiGetOperation) {
		return ListenableActionFutureWrapper.wrap(execute(multiGetOperation),
				new Function<MultiGetResponse, MultiGetResponseWrapper>() {

					@Override
					public MultiGetResponseWrapper apply(MultiGetResponse input) {
						return new DefaultMultiGetResponseWrapper(input, AbstractIndex.this);
					}

				});
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
			Esi4JOperation<ListenableActionFuture<SearchResponse>> searchOperation) {
		return ListenableActionFutureWrapper.wrap(execute(searchOperation),
				new Function<SearchResponse, SearchResponseWrapper>() {

					@Override
					public SearchResponseWrapper apply(SearchResponse response) {
						return new DefaultSearchResponseWrapper(response, AbstractIndex.this);
					}
				});
	}

	@Override
	public ListenableActionFuture<CountResponseWrapper> count(QueryBuilder query, Class<?> type) {
		return executeCount(new Count(query, type));
	}

	@Override
	public ListenableActionFuture<CountResponseWrapper> executeCount(
			Esi4JOperation<ListenableActionFuture<CountResponse>> countOperation) {

		return ListenableActionFutureWrapper.wrap(execute(countOperation),
				new Function<CountResponse, CountResponseWrapper>() {

					@Override
					public CountResponseWrapper apply(CountResponse response) {
						return new DefaultCountResponseWrapper(response);
					}
				});
	}

	@Override
	public final Object read(SearchHit hit) {
		return findTypeMapping(hit.index(), hit.type()).read(hit, this);
	}

	@Override
	public Object read(MultiGetItemResponse response) {
		return findTypeMapping(response.index(), response.type()).read(response.getResponse(), this);
	}

	protected static final class Search implements Esi4JOperation<ListenableActionFuture<SearchResponse>> {

		private final QueryBuilder _query;
		private final Class<?> _type;

		private final int _from;
		private final int _size;

		private Search(QueryBuilder query, Class<?> type) {
			this(query, type, 0, 10);
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
		public ListenableActionFuture<SearchResponse> execute(Client client, String indexName, OperationContext helper) {
			final TypeMapping typeMapping = helper.findTypeMapping(_type);

			final String type = typeMapping.getTypeAlias();

			SearchRequestBuilder builder = client.prepareSearch(indexName).setTypes(type).setQuery(_query)
					.setFrom(_from).setSize(_size);

			return builder.execute();
		}

	}

	protected static final class Count implements Esi4JOperation<ListenableActionFuture<CountResponse>> {

		private final QueryBuilder _query;
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
		public ListenableActionFuture<CountResponse> execute(Client client, String indexName, OperationContext helper) {
			final TypeMapping typeMapping = helper.findTypeMapping(_type);

			final String type = typeMapping.getTypeAlias();

			CountRequestBuilder builder = client.prepareCount(indexName).setTypes(type).setQuery(_query);

			return builder.execute();
		}

	}

	protected abstract Esi4JStore getStore();

	private static final class Get implements Esi4JOperation<ListenableActionFuture<GetResponse>> {

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
		public ListenableActionFuture<GetResponse> execute(Client client, String indexName, OperationContext helper) {
			final TypeMapping typeMapping = helper.findTypeMapping(_type);

			final String type = typeMapping.getTypeAlias();
			final String id = typeMapping.toIdString(_id);

			// FIXME doesn't work for multi index
			return client.prepareGet(indexName, type, id).execute();
		}
	}

	private static final class MultiGet implements Esi4JOperation<ListenableActionFuture<MultiGetResponse>> {

		private final Class<?> _type;
		private final Iterable<?> _ids;

		private MultiGet(Class<?> type, Iterable<?> ids) {
			if (type == null) {
				throw new NullPointerException("type");
			}
			if (ids == null) {
				throw new NullPointerException("ids");
			}
			_type = type;
			_ids = ids;
		}

		@Override
		public ListenableActionFuture<MultiGetResponse> execute(Client client, String indexName, OperationContext helper) {
			final TypeMapping typeMapping = helper.findTypeMapping(_type);
			final String type = typeMapping.getTypeAlias();

			MultiGetRequestBuilder builder = client.prepareMultiGet();
			for (Object id : _ids) {
				// FIXME doesn't work for multi index
				builder.add(indexName, type, typeMapping.toIdString(id));
			}

			return builder.execute();
		}

	}
}
