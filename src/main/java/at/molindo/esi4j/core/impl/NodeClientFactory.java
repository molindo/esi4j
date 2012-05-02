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
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import at.molindo.esi4j.core.Esi4JClient;
import at.molindo.esi4j.core.Esi4JClientFactory;

/**
 * uses a {@link NodeBuilder} to construct a {@link Node}. All settings are
 * directly passed to the {@link NodeBuilder}.
 */
public class NodeClientFactory implements Esi4JClientFactory {

	private final Settings _settings;
	private final String _clusterName;

	public NodeClientFactory(Settings settings) {
		_settings = settings;
		_clusterName = settings.get("cluster.name", ClusterName.DEFAULT.value());
	}

	@Override
	public Esi4JClient create() {
		return new NodeClient(_clusterName, NodeBuilder.nodeBuilder().settings(_settings).build());
	}

}
