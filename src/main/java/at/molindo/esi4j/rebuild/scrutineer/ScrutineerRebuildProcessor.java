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

import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.molindo.esi4j.core.Esi4JOperation;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.rebuild.Esi4JRebuildProcessor;
import at.molindo.esi4j.rebuild.Esi4JRebuildSession;
import at.molindo.esi4j.rebuild.util.BulkIndexHelper;
import at.molindo.scrutineer.IdAndVersionFactory;
import at.molindo.scrutineer.IdAndVersionStream;
import at.molindo.scrutineer.IdAndVersionStreamVerifier;
import at.molindo.scrutineer.IdAndVersionStreamVerifierListener;
import at.molindo.scrutineer.sort.SortedIdAndVersionStream;

public class ScrutineerRebuildProcessor implements Esi4JRebuildProcessor {

	private static final int DEFAULT_BATCH_SIZE = 1000;

	private static final Logger log = LoggerFactory.getLogger(ScrutineerRebuildProcessor.class);

	@Override
	public boolean isSupported(final Esi4JRebuildSession rebuildSession) {
		return true;
	}

	@Override
	public void rebuild(final InternalIndex index, final Esi4JRebuildSession rebuildSession) {
		index.updateMapping(rebuildSession.getType());

		index.execute(new Esi4JOperation<Void>() {

			@Override
			public Void execute(final Client client, final String indexName, final Esi4JOperation.OperationContext context) {

				final TypeMapping mapping = context.findTypeMapping(rebuildSession.getType());
				final IdAndVersionFactory factory = new MappedObjectIdAndVersionFactory(mapping);

				final IdAndVersionStream moduleStream = SortedIdAndVersionStream
						.wrapIfNecessary(new ModuleIdAndVersionStream(rebuildSession, DEFAULT_BATCH_SIZE, mapping), factory);

				final IdAndVersionStream esStream = SortedIdAndVersionStream
						.wrapIfNecessary(new ElasticsearchIdAndVersionStream(client, indexName, mapping
								.getTypeAlias(), factory), factory);

				final long start = System.currentTimeMillis();

				final BulkIndexHelper h = new BulkIndexHelper().setMaxRunning(2);
				h.setResponseHandler(new BulkIndexHelper.IResponseHandler() {

					@Override
					public void handle(final String id, final String type) {
						if ("delete".equals(type)) {
							onDelete(mapping.getTypeClass(), mapping.toId(id));
						} else {
							onIndex(mapping.getTypeClass(), mapping.toId(id));
						}
					}

				});

				final VerifierListener listener = new VerifierListener(client, indexName, context, mapping, h, DEFAULT_BATCH_SIZE);

				try {
					verify(moduleStream, esStream, listener);
				} finally {
					/*
					 * TODO let IdAndVersionStreamVerifier do that as it does with streams
					 */
					listener.close();
				}

				try {
					h.await();

					final long seconds = (System.currentTimeMillis() - start) / 1000;

					// logging
					final StringBuilder logMsg = new StringBuilder("finished indexing of ").append(h.getSucceeded())
							.append(" objects of type ").append(rebuildSession.getType().getName()).append(" in ")
							.append(seconds).append(" seconds");

					if (h.getFailed() > 0) {
						logMsg.append(" (").append(h.getFailed()).append(" failed)");
						log.warn(logMsg.toString());
					} else {
						log.info(logMsg.toString());
					}

				} catch (final InterruptedException e) {
					log.error("awaiting completion of indexing has been interrupted", e);
				}

				return null;
			}
		});
	}

	private void verify(final IdAndVersionStream primaryStream, final IdAndVersionStream secondaryStream, final IdAndVersionStreamVerifierListener listener) {
		new IdAndVersionStreamVerifier().verify(primaryStream, secondaryStream, listener);
	}

	protected void onIndex(final Class<?> type, final Object id) {
	}

	protected void onDelete(final Class<?> type, final Object id) {
	}

}
