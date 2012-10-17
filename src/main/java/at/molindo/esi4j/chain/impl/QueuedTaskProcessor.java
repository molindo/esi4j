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

import at.molindo.esi4j.chain.Esi4JEntityResolver;
import at.molindo.esi4j.chain.Esi4JEntityTask;
import at.molindo.esi4j.chain.Esi4JTaskProcessor;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.utils.collections.ArrayUtils;

/**
 * {@link Esi4JTaskProcessor} that uses a {@link QueuedTaskExecutor} to process
 * tasks asynchronously
 */
public class QueuedTaskProcessor extends AbstractTaskProcessor implements Esi4JTaskProcessor {

	private final QueuedTaskExecutor _executor;

	public QueuedTaskProcessor(Esi4JIndex index, Esi4JEntityResolver entityResolver) {
		super(index);
		_executor = new QueuedTaskExecutor(this, entityResolver);
	}

	public Esi4JEntityResolver getEntityResolver() {
		return _executor.getEntityResolver();
	}

	@Override
	public void processTasks(final Esi4JEntityTask[] tasks) {
		if (ArrayUtils.empty(tasks)) {
			return;
		}

		_executor.execute(tasks);

	}

	@Override
	public <T> T execute(SerializableEsi4JOperation<T> operation) {
		return _executor.submit(operation);
	}

	/**
	 * make sure to always call {@link #onAfterBulkIndex()} afterwards
	 */
	protected void onBeforeBulkIndex() {
	}

	protected void onAfterBulkIndex() {
	}

	@Override
	public void close() {
		_executor.close();
		super.close();
	}

}
