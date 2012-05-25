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
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.Esi4JOperation;
import at.molindo.esi4j.mapping.TypeMapping;

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

	public BulkIndexHelper() {
	}

	public void bulkIndex(Esi4JIndex index, final List<?> list) {
		bulkIndex(index.execute(new Esi4JOperation<BulkRequestBuilder>() {

			@Override
			public BulkRequestBuilder execute(Client client, String indexName, Esi4JOperation.OperationContext helper) {
				BulkRequestBuilder request = client.prepareBulk();
				for (Object o : list) {
					TypeMapping mapping = helper.findTypeMapping(o);
					if (!mapping.isFiltered(o)) {
						IndexRequestBuilder index = mapping.indexRequest(client, indexName, o);
						if (index != null) {
							request.add(index);
						}
					}
				}
				return request;
			}

		}));
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
					if (items[i].failed()) {
						failed++;
					} else {
						succeeded++;
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

}
