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

import org.elasticsearch.client.Client;

import at.molindo.esi4j.core.Esi4JIndex.IndexOperation;
import at.molindo.esi4j.core.Esi4JIndex.OperationHelper;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.module.Esi4JModule;

/**
 * a simple rebuild strategy that drops and recreates the index
 */
public class SimpleRebuildProcessor implements Esi4JRebuildProcessor {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SimpleRebuildProcessor.class);

	private static final int DEFAULT_BATCH_SIZE = 1000;

	private int _batchSize = DEFAULT_BATCH_SIZE;

	public SimpleRebuildProcessor() {
	}

	public SimpleRebuildProcessor(int batchSize) {
		setBatchSize(batchSize);
	}

	@Override
	public void rebuild(Esi4JModule module, InternalIndex index, Class<?>... types) {

		// TODO modify refresh interval?

		// rebuild type by type
		for (Class<?> type : types) {
			rebuildType(module, index, type);
		}

		// optimize index after rebuild
		index.execute(new IndexOperation<Void>() {

			@Override
			public Void execute(Client client, String indexName, OperationHelper helper) {
				client.admin().indices().prepareOptimize(indexName).execute().actionGet();
				return null;
			}
		});

	}

	private <T> void rebuildType(Esi4JModule module, InternalIndex index, final Class<T> type) {

		log.info("rebuilding index for object of type " + type.getName());

		long start = System.currentTimeMillis();

		RebuildSession<T> session = module.startRebuildSession(type);

		index.execute(new IndexOperation<Void>() {

			@Override
			public Void execute(Client client, String indexName, OperationHelper helper) {
				TypeMapping mapping = helper.findTypeMapping(type);
				client.admin().indices().prepareDeleteMapping(indexName).setType(mapping.getTypeAlias()).execute()
						.actionGet();
				return null;
			}
		});

		index.updateMapping(type);

		try {

			BulkIndexHelper h = new BulkIndexHelper(index);

			List<T> list;
			while ((list = session.getNext(_batchSize)).size() > 0) {
				h.bulkIndex(list);
			}

			h.await();

			long seconds = (System.currentTimeMillis() - start) / 1000;

			log.info("finished indexing of " + h.getIndexed() + " objects of type " + type.getName() + " in " + seconds
					+ " seconds (" + h.getFailed() + " failed)");

		} catch (InterruptedException e) {
			log.error("awaiting completion of indexing has been interrupted", e);
		} finally {
			session.close();
		}
	}

	public int getBatchSize() {
		return _batchSize;
	}

	@Override
	public void close() {
	}

	public SimpleRebuildProcessor setBatchSize(int batchSize) {
		if (batchSize <= 0) {
			throw new IllegalArgumentException("batchSize must be > 0, was " + batchSize);
		}
		_batchSize = batchSize;
		return this;
	}

}
