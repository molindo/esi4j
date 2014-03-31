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
package at.molindo.esi4j.rebuild.scrutineer;

import org.apache.commons.lang.SystemUtils;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.molindo.esi4j.core.Esi4JOperation;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.rebuild.Esi4JRebuildProcessor;
import at.molindo.esi4j.rebuild.Esi4JRebuildSession;
import at.molindo.esi4j.rebuild.util.BulkIndexHelper;

import com.aconex.scrutineer.IdAndVersion;
import com.aconex.scrutineer.IdAndVersionFactory;
import com.aconex.scrutineer.IdAndVersionStream;
import com.aconex.scrutineer.IdAndVersionStreamVerifierListener;
import com.aconex.scrutineer.elasticsearch.ElasticSearchDownloader;
import com.aconex.scrutineer.elasticsearch.ElasticSearchIdAndVersionStream;
import com.aconex.scrutineer.elasticsearch.ElasticSearchSorter;
import com.aconex.scrutineer.elasticsearch.IdAndVersionDataReaderFactory;
import com.aconex.scrutineer.elasticsearch.IdAndVersionDataWriterFactory;
import com.aconex.scrutineer.elasticsearch.IteratorFactory;
import com.fasterxml.sort.DataReaderFactory;
import com.fasterxml.sort.DataWriterFactory;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;
import com.fasterxml.sort.util.NaturalComparator;

public class ScrutineerRebuildProcessor implements Esi4JRebuildProcessor {

	private static final int DEFAULT_SORT_MEM = 256 * 1024 * 1024;
	private static final int DEFAULT_BATCH_SIZE = 1000;

	private static final Logger log = LoggerFactory.getLogger(ScrutineerRebuildProcessor.class);

	@Override
	public boolean isSupported(Esi4JRebuildSession rebuildSession) {
		return rebuildSession.isOrdered();
	}

	@Override
	public void rebuild(InternalIndex index, final Esi4JRebuildSession rebuildSession) {
		index.updateMapping(rebuildSession.getType());

		index.execute(new Esi4JOperation<Void>() {

			@Override
			public Void execute(Client client, String indexName, Esi4JOperation.OperationContext context) {

				final TypeMapping mapping = context.findTypeMapping(rebuildSession.getType());

				ModuleIdAndVersionStream moduleStream = new ModuleIdAndVersionStream(rebuildSession,
						DEFAULT_BATCH_SIZE, mapping);

				IdAndVersionFactory factory = new MappedObjectIdAndVersionFactory(mapping);

				ElasticSearchSorter elasticSearchSorter = new ElasticSearchSorter(createSorter(factory));
				IteratorFactory iteratorFactory = new IteratorFactory(factory);
				String workingDirectory = SystemUtils.getJavaIoTmpDir().getAbsolutePath();

				ElasticSearchIdAndVersionStream esStream = new ElasticSearchIdAndVersionStream(
						new ElasticSearchDownloader(client, indexName, "_type:" + mapping.getTypeAlias(), factory),
						elasticSearchSorter, iteratorFactory, workingDirectory);

				long start = System.currentTimeMillis();

				BulkIndexHelper h = new BulkIndexHelper().setMaxRunning(2);
				h.setResponseHandler(new BulkIndexHelper.IResponseHandler() {

					@Override
					public void handle(String id, String type) {
						if ("delete".equals(type)) {
							onDelete(mapping.getTypeClass(), mapping.toId(id));
						} else if ("index".equals(type)) {
							onIndex(mapping.getTypeClass(), mapping.toId(id));
						} else if ("create".equals(type)) {
							onCreate(mapping.getTypeClass(), mapping.toId(id));
						} else {
							log.warn("unexpected operation type {}", type);
						}
					}
				});

				VerifierListener listener = new VerifierListener(client, indexName, context, mapping, h,
						DEFAULT_BATCH_SIZE);

				try {
					verify(moduleStream, esStream, listener);
				} finally {
					/*
					 * TODO let IdAndVersionStreamVerifier do that as it does
					 * with streams
					 */
					listener.close();
				}

				try {
					h.await();

					long seconds = (System.currentTimeMillis() - start) / 1000;

					// logging
					StringBuilder logMsg = new StringBuilder("finished indexing of ").append(h.getSucceeded())
							.append(" objects of type ").append(rebuildSession.getType().getName()).append(" in ")
							.append(seconds).append(" seconds");

					if (h.getFailed() > 0) {
						logMsg.append(" (").append(h.getFailed()).append(" failed)");
						log.warn(logMsg.toString());
					} else {
						log.info(logMsg.toString());
					}

				} catch (InterruptedException e) {
					log.error("awaiting completion of indexing has been interrupted", e);
				}

				return null;
			}
		});
	}

	private Sorter<IdAndVersion> createSorter(IdAndVersionFactory factory) {
		SortConfig sortConfig = new SortConfig().withMaxMemoryUsage(DEFAULT_SORT_MEM);
		DataReaderFactory<IdAndVersion> dataReaderFactory = new IdAndVersionDataReaderFactory(factory);
		DataWriterFactory<IdAndVersion> dataWriterFactory = new IdAndVersionDataWriterFactory();
		return new Sorter<IdAndVersion>(sortConfig, dataReaderFactory, dataWriterFactory,
				new NaturalComparator<IdAndVersion>());
	}

	private void verify(IdAndVersionStream primaryStream, IdAndVersionStream secondaryStream,
			IdAndVersionStreamVerifierListener listener) {
		new IdAndVersionStreamVerifier().verify(primaryStream, secondaryStream, listener);
	}

	protected void onIndex(Class<?> type, Object id) {
	}

	protected void onDelete(Class<?> type, Object id) {
	}

	protected void onCreate(Class<?> type, Object id) {
		onIndex(type, id);
	}

}
