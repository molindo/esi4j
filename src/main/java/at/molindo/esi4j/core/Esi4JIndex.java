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
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;

import at.molindo.esi4j.action.BulkResponseWrapper;
import at.molindo.esi4j.action.DeleteResponseWrapper;
import at.molindo.esi4j.action.GetResponseWrapper;
import at.molindo.esi4j.action.IndexResponseWrapper;

/**
 * Interface exposed to users
 */
public interface Esi4JIndex extends Esi4JManagedIndex {

	/**
	 * 
	 * @return
	 */
	@Override
	String getName();

	ListenableActionFuture<IndexResponseWrapper> index(Object o);

	ListenableActionFuture<IndexResponseWrapper> executeIndex(
			Esi4JOperation<ListenableActionFuture<IndexResponse>> indexOperation);

	ListenableActionFuture<GetResponseWrapper> get(Class<?> type, Object id);

	ListenableActionFuture<GetResponseWrapper> executeGet(
			Esi4JOperation<ListenableActionFuture<GetResponse>> getOperation);

	ListenableActionFuture<DeleteResponseWrapper> delete(Object object);

	ListenableActionFuture<DeleteResponseWrapper> delete(Class<?> type, Object id);

	ListenableActionFuture<DeleteResponseWrapper> executeDelete(
			Esi4JOperation<ListenableActionFuture<DeleteResponse>> deleteOperation);

	ListenableActionFuture<BulkResponseWrapper> bulkIndex(Iterable<?> iterable);

	ListenableActionFuture<BulkResponseWrapper> executeBulk(
			Esi4JOperation<ListenableActionFuture<BulkResponse>> bulkOperation);

	/**
	 * refresh elasticsearch index, see <a href=
	 * "http://www.elasticsearch.org/guide/reference/api/admin-indices-refresh.html"
	 * >Refresh API</a>
	 */
	void refresh();

	void close();

}
