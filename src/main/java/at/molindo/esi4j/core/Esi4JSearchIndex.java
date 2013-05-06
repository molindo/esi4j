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
package at.molindo.esi4j.core;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;

import at.molindo.esi4j.action.CountResponseWrapper;
import at.molindo.esi4j.action.GetResponseWrapper;
import at.molindo.esi4j.action.MultiGetResponseWrapper;
import at.molindo.esi4j.action.SearchResponseWrapper;

public interface Esi4JSearchIndex {

	String getName();

	boolean isMapped(Class<?> type);

	boolean isMapped(Object o);

	Class<?>[] getMappedTypes();

	<T> T execute(Esi4JOperation<T> operation);

	ListenableActionFuture<GetResponseWrapper> get(Class<?> type, Object id);

	ListenableActionFuture<GetResponseWrapper> executeGet(
			Esi4JOperation<ListenableActionFuture<GetResponse>> getOperation);

	ListenableActionFuture<MultiGetResponseWrapper> multiGet(Class<?> type, Iterable<?> ids);

	ListenableActionFuture<MultiGetResponseWrapper> executeMultiGet(
			Esi4JOperation<ListenableActionFuture<MultiGetResponse>> multiGetOperation);

	ListenableActionFuture<SearchResponseWrapper> search(QueryBuilder query, Class<?> type);

	ListenableActionFuture<SearchResponseWrapper> search(QueryBuilder query, Class<?> type, int from, int size);

	ListenableActionFuture<SearchResponseWrapper> executeSearch(
			Esi4JOperation<ListenableActionFuture<SearchResponse>> searchOperation);

	ListenableActionFuture<CountResponseWrapper> count(QueryBuilder query, Class<?> type);

	ListenableActionFuture<CountResponseWrapper> executeCount(
			Esi4JOperation<ListenableActionFuture<CountResponse>> countOperation);
}
