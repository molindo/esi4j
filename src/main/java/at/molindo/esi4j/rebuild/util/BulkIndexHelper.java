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

				BulkItemResponse[] items = response.items();
				for (int i = 0; i < items.length; i++) {

					BulkItemResponse item = items[i];

					if (item.failed()) {
						failed++;
					} else {
						succeeded++;

						if (_responseHandler != null) {
							_responseHandler.handle(item.id(), item.opType());
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

		private final List<Operation> _operations;

		public Session(Client client, String indexName, OperationContext context, int batchSize) {
			_client = client;
			_indexName = indexName;
			_context = context;
			_batchSize = batchSize;
			_operations = Lists.newArrayListWithCapacity(_batchSize);
		}

		public Session index(Object o) {
			add(new Index(o));
			return this;
		}

		public Session delete(Object o) {
			TypeMapping mapping = _context.findTypeMapping(o);
			delete(mapping.getTypeClass(), mapping.getId(o), mapping.getVersion(o));
			return this;
		}

		public Session delete(Class<?> type, Object id, Long version) {
			add(new Delete(type, id, version));
			return this;
		}

		private void add(Operation op) {
			_operations.add(op);
			if (_operations.size() == _batchSize) {
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
						for (Operation op : _operations) {

							Object request = op.toRequest(client, indexName, helper);
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
				_operations.clear();
			}
			return BulkIndexHelper.this;
		}

	}

	private interface Operation {

		Object toRequest(Client client, String indexName, OperationContext helper);

	}

	private static class Delete implements Operation {

		private final Class<?> _type;
		private final Object _id;
		private final Long _version;

		public Delete(Class<?> type, Object id, Long version) {
			if (type == null) {
				throw new NullPointerException("type");
			}
			if (id == null) {
				throw new NullPointerException("id");
			}
			_type = type;
			_id = id;
			_version = version;
		}

		@Override
		public DeleteRequestBuilder toRequest(Client client, String indexName, OperationContext helper) {
			TypeMapping mapping = helper.findTypeMapping(_type);
			return mapping.deleteRequest(client, indexName, mapping.toIdString(_id), _version);
		}

	}

	private static class Index implements Operation {

		private final Object _object;

		public Index(Object object) {
			if (object == null) {
				throw new NullPointerException("object");
			}
			_object = object;
		}

		@Override
		public IndexRequestBuilder toRequest(Client client, String indexName, OperationContext helper) {
			TypeMapping mapping = helper.findTypeMapping(_object);
			return mapping.indexRequest(client, indexName, _object);
		}

	}

}
