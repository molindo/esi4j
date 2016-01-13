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

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

import at.molindo.esi4j.action.BulkResponseWrapper;
import at.molindo.esi4j.chain.Esi4JEntityTask;
import at.molindo.esi4j.chain.Esi4JTaskProcessor;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.Esi4JOperation;

public class DefaultTaskProcessor extends AbstractTaskProcessor implements Esi4JTaskProcessor {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DefaultTaskProcessor.class);

	private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();

	public DefaultTaskProcessor(final Esi4JIndex index) {
		super(index);
	}

	@Override
	public void processTasks(final Esi4JEntityTask[] tasks) {
		_lock.readLock().lock();
		try {
			final BulkResponseWrapper response = getIndex()
					.executeBulk(new Esi4JOperation<ListenableActionFuture<BulkResponse>>() {

						@Override
						public ListenableActionFuture<BulkResponse> execute(final Client client, final String indexName, final OperationContext helper) {
							final BulkRequestBuilder bulk = client.prepareBulk();

							for (final Esi4JEntityTask task : tasks) {
								if (task != null) {
									task.addToBulk(client, bulk, indexName, helper);
								}
							}

							return bulk.execute();
						}
					}).actionGet();

			if (log.isDebugEnabled()) {
				log.debug("finished bulk indexing " + response.getBulkResponse().getItems().length + " items");
			}
		} finally {
			_lock.readLock().unlock();
		}
	}

	@Override
	public <T> T execute(final SerializableEsi4JOperation<T> operation) {
		_lock.writeLock().lock();
		try {
			final T value = getIndex().execute(operation);
			if (log.isDebugEnabled()) {
				log.debug("finished submitted operation");
			}
			return value;
		} finally {
			_lock.writeLock().unlock();
		}
	}

}
