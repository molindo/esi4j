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
package at.molindo.esi4j.rebuild.simple;

import java.util.List;

import org.elasticsearch.client.Client;

import at.molindo.esi4j.core.Esi4JOperation;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.rebuild.Esi4JRebuildProcessor;
import at.molindo.esi4j.rebuild.Esi4JRebuildSession;
import at.molindo.esi4j.rebuild.util.BulkIndexHelper;

/**
 * a simple rebuild strategy that drops and recreates the type
 */
public class SimpleRebuildProcessor implements Esi4JRebuildProcessor {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SimpleRebuildProcessor.class);

	private static final int DEFAULT_BATCH_SIZE = 1000;
	private static final int DEFAULT_MAX_RUNNING = 10;

	private int _batchSize = DEFAULT_BATCH_SIZE;
	private int _maxRunning = DEFAULT_MAX_RUNNING;

	public SimpleRebuildProcessor() {
	}

	public SimpleRebuildProcessor(final int batchSize) {
		setBatchSize(batchSize);
	}

	@Override
	public boolean isSupported(final Esi4JRebuildSession rebuildSession) {
		return true;
	}

	@Override
	public void rebuild(final InternalIndex index, final Esi4JRebuildSession rebuildSession) {

		final Class<?> type = rebuildSession.getType();

		log.info("rebuilding index for object of type " + type.getName());

		final long start = System.currentTimeMillis();

		index.execute(new Esi4JOperation<Void>() {

			@Override
			public Void execute(final Client client, final String indexName, final Esi4JOperation.OperationContext context) {
				final TypeMapping mapping = context.findTypeMapping(type);
				client.admin().indices().prepareDeleteMapping(indexName).setType(mapping.getTypeAlias()).execute()
						.actionGet();
				return null;
			}
		});

		index.updateMapping(type);

		try {

			final BulkIndexHelper h = new BulkIndexHelper().setMaxRunning(getMaxRunning());

			List<?> list;
			while ((list = rebuildSession.getNext(_batchSize)).size() > 0) {
				h.bulkIndex(index, list);
			}

			h.await();

			final long seconds = (System.currentTimeMillis() - start) / 1000;

			// logging
			final StringBuilder logMsg = new StringBuilder("finished indexing of ").append(h.getSucceeded())
					.append(" objects of type ").append(type.getName()).append(" in ").append(seconds)
					.append(" seconds");

			if (h.getFailed() > 0) {
				logMsg.append(" (").append(h.getFailed()).append(" failed)");
				log.warn(logMsg.toString());
			} else {
				log.info(logMsg.toString());
			}

		} catch (final InterruptedException e) {
			log.error("awaiting completion of indexing has been interrupted", e);
		}
	}

	public int getBatchSize() {
		return _batchSize;
	}

	public SimpleRebuildProcessor setBatchSize(final int batchSize) {
		if (batchSize <= 0) {
			throw new IllegalArgumentException("batchSize must be > 0, was " + batchSize);
		}
		_batchSize = batchSize;
		return this;
	}

	public int getMaxRunning() {
		return _maxRunning;
	}

	public SimpleRebuildProcessor setMaxRunning(final int maxRunning) {
		if (maxRunning <= 0) {
			throw new IllegalArgumentException("maxRunning must be > 0, was " + maxRunning);
		}
		_maxRunning = maxRunning;
		return this;
	}

}
