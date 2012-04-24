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
package at.molindo.esi4j.rebuild;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;

import at.molindo.esi4j.action.BulkResponseWrapper;
import at.molindo.esi4j.core.Esi4JIndex;

/**
 * helper class that helps awaiting completion of submitted bulk index tasks
 */
public class BulkIndexHelper {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(BulkIndexHelper.class);

	private static final int DEFAULT_MAX_RUNNING = 10;

	private final Esi4JIndex _index;

	private final ReentrantLock _lock = new ReentrantLock();
	private final Condition _allCompleted = _lock.newCondition();
	private final Condition _nextCompleted = _lock.newCondition();

	private int _maxRunning = DEFAULT_MAX_RUNNING;

	private int _running = 0;
	private int _indexed = 0;
	private int _failed = 0;

	public BulkIndexHelper(Esi4JIndex index) {
		if (index == null) {
			throw new NullPointerException("index");
		}
		_index = index;

	}

	/**
	 * bulk index all objects in list
	 * 
	 * @see Esi4JIndex#bulkIndex(Iterable)
	 */
	public void bulkIndex(List<?> list) {

		final int listSize = list.size();

		ListenableActionFuture<BulkResponseWrapper> future = _index.bulkIndex(list);

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

		// might execute immediately
		future.addListener(new ActionListener<BulkResponseWrapper>() {

			@Override
			public void onResponse(BulkResponseWrapper response) {
				int indexed = 0;
				int failed = 0;

				BulkItemResponse[] items = response.getBulkResponse().items();
				for (int i = 0; i < items.length; i++) {
					if (items[i].failed()) {
						failed++;
					} else {
						indexed++;
					}
				}

				end(indexed, failed);
			}

			@Override
			public void onFailure(Throwable e) {
				log.warn("failed to bulk index", e);
				end(0, listSize);
			}

			private void end(int indexed, int failed) {
				_lock.lock();
				try {
					_failed += failed;
					_indexed += indexed;
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
	 * @return number of indexed objects
	 */
	public int getIndexed() {
		_lock.lock();
		try {
			return _indexed;
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
