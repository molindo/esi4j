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
package at.molindo.esi4j.rebuild.impl;

import java.util.List;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;

import at.molindo.esi4j.core.Esi4JOperation;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.module.Esi4JModule;
import at.molindo.esi4j.rebuild.Esi4JRebuildManager;
import at.molindo.esi4j.rebuild.Esi4JRebuildProcessor;
import at.molindo.esi4j.rebuild.Esi4JRebuildSession;
import at.molindo.esi4j.rebuild.scrutineer.ScrutineerRebuildProcessor;
import at.molindo.esi4j.rebuild.simple.SimpleRebuildProcessor;
import at.molindo.utils.collections.ArrayUtils;

import com.google.common.collect.Lists;

public class DefaultRebuildManager implements Esi4JRebuildManager {

	private final List<Esi4JRebuildProcessor> _processors;

	private TimeValue _healthTimeout = TimeValue.timeValueSeconds(60);

	public DefaultRebuildManager() {
		this(new ScrutineerRebuildProcessor(), new SimpleRebuildProcessor());
	}

	public DefaultRebuildManager(Esi4JRebuildProcessor... processors) {
		if (ArrayUtils.empty(processors)) {
			throw new IllegalArgumentException("processors must not be empty");
		}
		_processors = Lists.newArrayList(processors);
	}

	@Override
	public void rebuild(Esi4JModule module, InternalIndex index, Class<?>... types) {
		for (Class<?> type : types) {
			// validate types before continuing
			index.findTypeMapping(type);
		}

		// TODO assert index - but if failed? restart? clone?

		// TODO clone store?

		// TODO how to handle events while rebuilding clone?

		for (Class<?> type : types) {
			Esi4JRebuildSession rebuildSession = module.startRebuildSession(type);
			try {
				waitForGreenStatus(index);
				findRebuildProcessor(rebuildSession).rebuild(index, rebuildSession);

				// update metadata after succesful build
				Object meta = rebuildSession.getMetadata();
				if (meta != null) {
					index.index(meta);
				}

			} finally {
				rebuildSession.close();
			}

		}

		// optimize index after rebuild
		index.execute(new Esi4JOperation<Void>() {

			@Override
			public Void execute(Client client, String indexName, OperationContext context) {
				client.admin().indices().prepareOptimize(indexName).execute().actionGet();
				return null;
			}
		});
	}

	private void waitForGreenStatus(InternalIndex index) {
		if (_healthTimeout != null && _healthTimeout.seconds() > 0) {
			// make sure the index is ready
			index.execute(new Esi4JOperation<Void>() {

				@Override
				public Void execute(Client client, String indexName, OperationContext helper) {
					ClusterHealthRequestBuilder request = client.admin().cluster().prepareHealth(indexName);

					request.setWaitForGreenStatus().setTimeout(_healthTimeout);

					ClusterHealthResponse response = request.execute().actionGet();

					if (response.getStatus() != ClusterHealthStatus.GREEN) {
						throw new IllegalStateException("cluster not ready for rebuild (status " + response.getStatus()
								+ ")");
					}

					return null;
				}
			});
		}
	}

	/**
	 * @return first {@link Esi4JRebuildProcessor} that supports this
	 *         {@link TypeMapping}
	 */
	private Esi4JRebuildProcessor findRebuildProcessor(Esi4JRebuildSession rebuildSession) {
		for (Esi4JRebuildProcessor processor : _processors) {
			if (processor.isSupported(rebuildSession)) {
				return processor;
			}
		}
		throw new IllegalArgumentException("session for type " + rebuildSession.getType()
				+ " not supported by any processor");
	}

	@Override
	public void close() {
	}

	public TimeValue getHealthTimeout() {
		return _healthTimeout;
	}

	public DefaultRebuildManager setHealthTimeout(TimeValue healthTimeout) {
		_healthTimeout = healthTimeout;
		return this;
	}

}
