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
package at.molindo.esi4j.rebuild.util;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.Esi4JOperation;
import at.molindo.esi4j.core.Esi4JOperation.OperationContext;
import at.molindo.esi4j.mapping.TypeMapping;

import com.google.common.collect.Lists;

/**
 * helper class that helps awaiting completion of submitted bulk index tasks
 */
public class BulkIndexHelper {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(BulkIndexHelper.class);

	private static final int DEFAULT_MAX_RUNNING = 2;

	private final ReentrantLock _lock = new ReentrantLock();
	private final Condition _allCompleted = _lock.newCondition();
	private final Condition _nextCompleted = _lock.newCondition();

	private int _maxRunning = DEFAULT_MAX_RUNNING;

	private int _running = 0;
	private int _succeeded = 0;
	private int _failed = 0;

	private IResponseHandler _responseHandler;

	public BulkIndexHelper() {
	}

	public Session newSession(Esi4JIndex index, final int batchSize) {
		return index.execute(new Esi4JOperation<Session>() {

			@Override
			public Session execute(Client client, String indexName, OperationContext helper) {

				return newSession(client, indexName, helper, batchSize);
			}

		});
	}

	public Session newSession(Client client, String indexName, OperationContext context, int batchSize) {
		return new Session(client, indexName, context, batchSize);
	}

	public void bulkIndex(Esi4JIndex index, final List<?> list) {
		Session session = newSession(index, list.size());
		for (Object o : list) {
			session.index(o);
		}
		session.submit();
	}

	public void bulkIndex(BulkRequestBuilder request) {
		final int items = request.numberOfActions();
		if (items == 0) {
			// nothing to do
			return;
		}

		_lock.lock();
		try {
			while (_running >= _maxRunning) {
				try {
					_nextCompleted.await();
				} catch (InterruptedException e) {
					throw new RuntimeException("waiting interrupted", e);
				}
			}
			_running++;
		} finally {
			_lock.unlock();
		}

		request.execute(new ActionListener<BulkResponse>() {

			@Override
			public void onResponse(BulkResponse response) {
				int succeeded = 0;
				int failed = 0;

				BulkItemResponse[] items = response.getItems();
				for (int i = 0; i < items.length; i++) {

					BulkItemResponse item = items[i];

					if (item.isFailed()) {
						failed++;
					} else {
						succeeded++;

						if (_responseHandler != null) {
							_responseHandler.handle(item.getId(), item.getOpType());
						}
					}
				}

				end(succeeded, failed);
			}

			@Override
			public void onFailure(Throwable e) {
				log.warn("failed to bulk index", e);
				end(0, items);
			}

			private void end(int succeeded, int failed) {
				_lock.lock();
				try {
					_failed += failed;
					_succeeded += succeeded;
					_nextCompleted.signal();
					if (--_running == 0) {
						_allCompleted.signal();
					}
				} finally {
					_lock.unlock();
				}
			}
		});
	}

	/**
	 * await termination of all running batch index tasks
	 */
	public void await() throws InterruptedException {
		_lock.lock();
		try {
			while (_running > 0) {
				_allCompleted.await();
			}
		} finally {
			_lock.unlock();
		}
	}

	/**
	 * @return number of succeeded objects
	 */
	public int getSucceeded() {
		_lock.lock();
		try {
			return _succeeded;
		} finally {
			_lock.unlock();
		}
	}

	/**
	 * @return number of failed objects
	 */
	public int getFailed() {
		_lock.lock();
		try {
			return _failed;
		} finally {
			_lock.unlock();
		}
	}

	public int getMaxRunning() {
		return _maxRunning;
	}

	public BulkIndexHelper setMaxRunning(int maxRunning) {
		if (maxRunning <= 0) {
			throw new IllegalArgumentException("maxRunning must be > 0, was " + maxRunning);
		}

		_maxRunning = maxRunning;
		return this;
	}

	public BulkIndexHelper setResponseHandler(IResponseHandler responseHandler) {
		_responseHandler = responseHandler;
		return this;
	}

	public interface IResponseHandler {
		void handle(String id, String type);
	}

	public class Session {

		private final Client _client;
		private final String _indexName;
		private final OperationContext _context;
		private final int _batchSize;

		private final List<ActionRequestBuilder<?, ?, ?>> _requests;

		public Session(Client client, String indexName, OperationContext context, int batchSize) {
			_client = client;
			_indexName = indexName;
			_context = context;
			_batchSize = batchSize;
			_requests = Lists.newArrayListWithCapacity(_batchSize);
		}

		public Session index(Object o) {
			add(toIndexRequest(o));
			return this;
		}

		public IndexRequestBuilder toIndexRequest(Object object) {
			TypeMapping mapping = _context.findTypeMapping(object);
			return mapping.indexRequest(_client, _indexName, object);
		}

		public Session delete(Object o) {
			TypeMapping mapping = _context.findTypeMapping(o);
			delete(mapping.getTypeClass(), mapping.getId(o), mapping.getVersion(o));
			return this;
		}

		public Session delete(Class<?> type, Object id, Long version) {
			add(toDeleteRequest(type, id, version));
			return this;
		}

		private DeleteRequestBuilder toDeleteRequest(Class<?> type, Object id, Long version) {
			TypeMapping mapping = _context.findTypeMapping(type);
			return mapping.deleteRequest(_client, _indexName, mapping.toIdString(id), version);
		}

		private void add(ActionRequestBuilder<?, ?, ?> request) {
			_requests.add(request);
			if (_requests.size() == _batchSize) {
				submit();
			}
		}

		public BulkIndexHelper submit() {
			try {
				// build BulkRequestBuilder and clear operations
				bulkIndex(new Esi4JOperation<BulkRequestBuilder>() {

					@Override
					public BulkRequestBuilder execute(Client client, String indexName,
							Esi4JOperation.OperationContext helper) {
						BulkRequestBuilder bulk = client.prepareBulk();
						for (ActionRequestBuilder<?, ?, ?> request : _requests) {
							if (request instanceof IndexRequestBuilder) {
								bulk.add((IndexRequestBuilder) request);
							} else if (request instanceof DeleteRequestBuilder) {
								bulk.add((DeleteRequestBuilder) request);
							} else if (request != null) {
								throw new IllegalArgumentException("unexpected request type "
										+ request.getClass().getName());
							}

						}
						return bulk;
					}

				}.execute(_client, _indexName, _context));
			} finally {
				_requests.clear();
			}
			return BulkIndexHelper.this;
		}

	}

}
