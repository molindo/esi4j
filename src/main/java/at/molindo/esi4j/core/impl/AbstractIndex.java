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
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import at.molindo.esi4j.action.CountResponseWrapper;
import at.molindo.esi4j.action.MultiGetItemResponseWrapper.MultiGetItemReader;
import at.molindo.esi4j.action.MultiGetResponseWrapper;
import at.molindo.esi4j.action.MultiSearchResponseWrapper;
import at.molindo.esi4j.action.SearchHitWrapper.SearchHitReader;
import at.molindo.esi4j.action.SearchResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultCountResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultMultiGetResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultMultiSearchResponseWrapper;
import at.molindo.esi4j.action.impl.DefaultSearchResponseWrapper;
import at.molindo.esi4j.core.Esi4JManagedIndex;
import at.molindo.esi4j.core.Esi4JOperation;
import at.molindo.esi4j.core.Esi4JOperation.OperationContext;
import at.molindo.esi4j.core.Esi4JSearchIndex;
import at.molindo.esi4j.core.Esi4JStore;
import at.molindo.esi4j.core.Esi4JStore.StoreOperation;
import at.molindo.esi4j.mapping.ObjectReadSource;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.util.ListenableActionFutureWrapper;
import at.molindo.utils.data.Function;

public abstract class AbstractIndex implements Esi4JSearchIndex, Esi4JManagedIndex, OperationContext, SearchHitReader, MultiGetItemReader {

	@Override
	public <T> T execute(final Esi4JOperation<T> operation) {
		return getStore().execute(new StoreOperation<T>() {

			@Override
			public T execute(final Client client, final String indexName) {
				return operation.execute(client, indexName, AbstractIndex.this);
			}
		});
	}

	@Override
	public ListenableActionFuture<SearchResponseWrapper> search(final QueryBuilder query, final Class<?> type) {
		return executeSearch(new Search(query, type));
	}

	@Override
	public ListenableActionFuture<SearchResponseWrapper> search(final QueryBuilder query, final Class<?> type, final int from, final int size) {
		return executeSearch(new Search(query, type, from, size));
	}

	@Override
	public ListenableActionFuture<SearchResponseWrapper> executeSearch(final Esi4JOperation<ListenableActionFuture<SearchResponse>> searchOperation) {
		return ListenableActionFutureWrapper
				.wrap(execute(searchOperation), new Function<SearchResponse, SearchResponseWrapper>() {

					@Override
					public SearchResponseWrapper apply(final SearchResponse response) {
						return new DefaultSearchResponseWrapper(response, AbstractIndex.this);
					}
				});
	}

	@Override
	public ListenableActionFuture<MultiSearchResponseWrapper> executeMultiSearch(final Esi4JOperation<ListenableActionFuture<MultiSearchResponse>> multiSearchOperation) {
		return ListenableActionFutureWrapper
				.wrap(execute(multiSearchOperation), new Function<MultiSearchResponse, MultiSearchResponseWrapper>() {

					@Override
					public MultiSearchResponseWrapper apply(final MultiSearchResponse response) {
						return new DefaultMultiSearchResponseWrapper(response, AbstractIndex.this);
					}
				});
	}

	@Override
	public ListenableActionFuture<CountResponseWrapper> count(final QueryBuilder query, final Class<?> type) {
		return executeCount(new Count(query, type));
	}

	@Override
	public ListenableActionFuture<CountResponseWrapper> executeCount(final Esi4JOperation<ListenableActionFuture<CountResponse>> countOperation) {

		return ListenableActionFutureWrapper
				.wrap(execute(countOperation), new Function<CountResponse, CountResponseWrapper>() {

					@Override
					public CountResponseWrapper apply(final CountResponse response) {
						return new DefaultCountResponseWrapper(response);
					}
				});
	}

	@Override
	public ListenableActionFuture<MultiGetResponseWrapper> multiGet(final Class<?> type, final Iterable<?> ids) {
		return executeMultiGet(new MultiGet(type, ids));
	}

	@Override
	public ListenableActionFuture<MultiGetResponseWrapper> executeMultiGet(final Esi4JOperation<ListenableActionFuture<MultiGetResponse>> multiGetOperation) {
		return ListenableActionFutureWrapper
				.wrap(execute(multiGetOperation), new Function<MultiGetResponse, MultiGetResponseWrapper>() {

					@Override
					public MultiGetResponseWrapper apply(final MultiGetResponse input) {
						return new DefaultMultiGetResponseWrapper(input, AbstractIndex.this);
					}

				});
	}

	@Override
	public final Object read(final SearchHit hit) {
		return findTypeMapping(hit.index(), hit.type()).read(ObjectReadSource.Builder.search(hit));
	}

	@Override
	public Object read(final MultiGetItemResponse response) {
		return findTypeMapping(response.getIndex(), response.getType())
				.read(ObjectReadSource.Builder.get(response.getResponse()));
	}

	protected abstract Esi4JStore getStore();

	protected static final class Search implements Esi4JOperation<ListenableActionFuture<SearchResponse>> {

		private final QueryBuilder _query;
		private final Class<?> _type;

		private final int _from;
		private final int _size;

		private Search(final QueryBuilder query, final Class<?> type) {
			this(query, type, 0, 10);
		}

		private Search(final QueryBuilder query, final Class<?> type, final int from, final int size) {
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
		public ListenableActionFuture<SearchResponse> execute(final Client client, final String indexName, final OperationContext helper) {
			final TypeMapping typeMapping = helper.findTypeMapping(_type);

			final String type = typeMapping.getTypeAlias();

			final SearchRequestBuilder builder = client.prepareSearch(indexName).setTypes(type).setQuery(_query)
					.setFrom(_from).setSize(_size);

			return builder.execute();
		}

	}

	protected static final class Count implements Esi4JOperation<ListenableActionFuture<CountResponse>> {

		private final QueryBuilder _query;
		private final Class<?> _type;

		private Count(final QueryBuilder query, final Class<?> type) {
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
		public ListenableActionFuture<CountResponse> execute(final Client client, final String indexName, final OperationContext helper) {
			final TypeMapping typeMapping = helper.findTypeMapping(_type);

			final String type = typeMapping.getTypeAlias();

			final CountRequestBuilder builder = client.prepareCount(indexName).setTypes(type).setQuery(_query);

			return builder.execute();
		}

	}

	private static final class MultiGet implements Esi4JOperation<ListenableActionFuture<MultiGetResponse>> {

		private final Class<?> _type;
		private final Iterable<?> _ids;

		private MultiGet(final Class<?> type, final Iterable<?> ids) {
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
		public ListenableActionFuture<MultiGetResponse> execute(final Client client, final String indexName, final OperationContext helper) {
			final TypeMapping typeMapping = helper.findTypeMapping(_type);
			final String type = typeMapping.getTypeAlias();

			final MultiGetRequestBuilder builder = client.prepareMultiGet();
			for (final Object id : _ids) {
				// ignore indexName as it may be a multi-index
				builder.add(helper.findIndexName(_type), type, typeMapping.toIdString(id));
			}

			return builder.execute();
		}

	}

}
