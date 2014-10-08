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
package at.molindo.esi4j.util;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.support.InternalTransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import at.molindo.thirdparty.org.springframework.scheduling.annotation.AsyncResult;

public class ListenableAsyncResult<V> extends AsyncResult<V> implements ListenableActionFuture<V> {

	private final ThreadPool _threadPool;

	public static <V> ListenableAsyncResult<V> create(V value) {
		return new ListenableAsyncResult<V>(value, null);
	}

	public static <V> ListenableAsyncResult<V> create(V value, Client client, ActionRequest<?> request) {
		return new ListenableAsyncResult<V>(value,
				request.listenerThreaded() ? ((InternalTransportClient) client).threadPool() : null);
	}

	public ListenableAsyncResult(V value, ThreadPool threadPool) {
		super(value);
		_threadPool = threadPool;
	}

	@Override
	public V actionGet() throws ElasticsearchException {
		return get();
	}

	@Override
	public V actionGet(String timeout) throws ElasticsearchException {
		return get();
	}

	@Override
	public V actionGet(long timeoutMillis) throws ElasticsearchException {
		return get();
	}

	@Override
	public V actionGet(long timeout, TimeUnit unit) throws ElasticsearchException {
		return get();
	}

	@Override
	public V actionGet(TimeValue timeout) throws ElasticsearchException {
		return get();
	}

	@Override
	public Throwable getRootFailure() {
		return null;
	}

	@Override
	public void addListener(final ActionListener<V> listener) {
		executeListener(listener);
	}

	private void executeListener(final ActionListener<V> listener) {
		executeListener(new Runnable() {

			@Override
			public void run() {
				listener.onResponse(get());
			}
		});
	}

	private void executeListener(final Runnable listener) {
		if (_threadPool != null) {
			_threadPool.generic().execute(listener);
		} else {
			listener.run();
		}
	}

}
