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
package at.molindo.esi4j.core.impl;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.NodeBuilder;

import at.molindo.esi4j.core.Esi4JClient;
import at.molindo.esi4j.core.Esi4JClientFactory;
import at.molindo.utils.collections.ArrayUtils;
import at.molindo.utils.data.StringUtils;

/**
 * Creates an {@link Esi4JClient} that uses a {@link TransportClient}. All settings are directly passed to the
 * {@link NodeBuilder}.
 */
public class TransportClientFactory implements Esi4JClientFactory {

	private final Settings _settings;
	private final String _clusterName;
	private final String[] _hosts;

	public TransportClientFactory(final Settings settings) {
		_settings = settings;
		_clusterName = settings.get("cluster.name", ClusterName.DEFAULT.value());
		// TODO make use of settings.getComponentSettings(..)
		_hosts = settings.getAsArray("esi4j.client.transport.hosts");

		if (ArrayUtils.empty(_hosts)) {
			throw new IllegalArgumentException("hosts required");
		}
	}

	@Override
	public Esi4JClient create() {
		final org.elasticsearch.client.transport.TransportClient client = new org.elasticsearch.client.transport.TransportClient(_settings);

		final String[] parts = new String[2];
		for (final String host : _hosts) {
			if (StringUtils.split(host, ":", parts) == 1) {
				parts[1] = "9300";
			}
			client.addTransportAddress(new InetSocketTransportAddress(parts[0], Integer.parseInt(parts[1])));
		}

		return new TransportClient(_clusterName, client);
	}
}
