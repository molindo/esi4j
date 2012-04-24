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
package at.molindo.esi4j.chain.impl;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

import at.molindo.esi4j.chain.Esi4JEntityResolver;
import at.molindo.esi4j.chain.Esi4JEntityTask;
import at.molindo.esi4j.core.Esi4JIndex.IndexOperation;
import at.molindo.esi4j.core.Esi4JIndex.OperationHelper;
import at.molindo.utils.collections.ArrayUtils;

public class QueuedTaskExecutor implements ThreadFactory, RejectedExecutionHandler {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(QueuedTaskExecutor.class);

	private static final AtomicInteger EXECUTOR_NUMBER = new AtomicInteger(1);

	private final int _executorNumber = EXECUTOR_NUMBER.getAndIncrement();
	private final AtomicInteger _threadNumber = new AtomicInteger(1);

	private final QueuedTaskProcessor _queuedTaskProcessor;
	private final Esi4JEntityResolver _entityResolver;
	private final LinkedBlockingQueue<Runnable> _queue;
	private final ExecutorService _executorService;

	public QueuedTaskExecutor(QueuedTaskProcessor queuedTaskProcessor, Esi4JEntityResolver entityResolver) {
		if (queuedTaskProcessor == null) {
			throw new NullPointerException("queuedTaskProcessor");
		}
		_queuedTaskProcessor = queuedTaskProcessor;
		_entityResolver = entityResolver;

		// TODO make configurable
		int poolSize = Runtime.getRuntime().availableProcessors();
		_queue = new LinkedBlockingQueue<Runnable>();
		_executorService = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, _queue, this, this);
	}

	public void execute(Esi4JEntityTask[] tasks) {
		if (!ArrayUtils.empty(tasks)) {
			if (_entityResolver != null) {
				for (int i = 0; i < tasks.length; i++) {
					tasks[i].replaceEntity(_entityResolver);
				}
			}
			_executorService.execute(new BulkIndexRunnable(tasks));
		}
	}

	public QueuedTaskProcessor getTaskProcessor() {
		return _queuedTaskProcessor;
	}

	public Esi4JEntityResolver getEntityResolver() {
		return _entityResolver;
	}

	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
		log.warn("executor rejected execution of bulk index task");
	}

	@Override
	public Thread newThread(Runnable r) {
		return new BulkIndexThread(r);
	}

	public void close() {
		_executorService.shutdown();
		try {
			_executorService.awaitTermination(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.warn("waiting for termination of executor service interrupted", e);
		}
	}

	private final class BulkIndexThread extends Thread {

		public BulkIndexThread(Runnable r) {
			super(r, QueuedTaskProcessor.class.getSimpleName() + "-" + _executorNumber + "-"
					+ _threadNumber.getAndIncrement());
			setDaemon(true);
		}

		public QueuedTaskExecutor getQueuedTaskExecutor() {
			return QueuedTaskExecutor.this;
		}

	}

	private static final class BulkIndexRunnable implements Runnable, Serializable {

		private static final long serialVersionUID = 1L;

		private final Esi4JEntityTask[] _tasks;

		public BulkIndexRunnable(Esi4JEntityTask[] tasks) {
			_tasks = tasks;
		}

		@Override
		public void run() {
			final QueuedTaskExecutor executor = ((BulkIndexThread) Thread.currentThread()).getQueuedTaskExecutor();

			executor.getTaskProcessor().onBeforeBulkIndex();

			Esi4JEntityResolver entityResolver = executor.getEntityResolver();

			if (entityResolver != null) {
				for (int i = 0; i < _tasks.length; i++) {
					_tasks[i].resolveEntity(entityResolver);
				}
			}

			// TODO handle response
			executor.getTaskProcessor().getIndex()
					.executeBulk(new IndexOperation<ListenableActionFuture<BulkResponse>>() {

						@Override
						public ListenableActionFuture<BulkResponse> execute(Client client, String indexName,
								OperationHelper helper) {
							BulkRequestBuilder bulk = client.prepareBulk();

							for (int i = 0; i < _tasks.length; i++) {
								_tasks[i].addToBulk(bulk, indexName, helper);
							}

							ListenableActionFuture<BulkResponse> response = bulk.execute();

							executor.getTaskProcessor().onAfterBulkIndex();

							return response;
						}
					});
		}

	}

}
