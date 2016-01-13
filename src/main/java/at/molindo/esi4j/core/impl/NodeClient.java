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

import org.elasticsearch.node.Node;

public class NodeClient extends TransportClient {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NodeClient.class);

	private final Node _node;

	public NodeClient(final String clusterName, final Node node) {
		super(clusterName, node.client());
		_node = node;
		_node.start();
	}

	public Node getNode() {
		return _node;
	}

	@Override
	public void close() {
		_node.close();
	}

	@Override
	protected void finalize() throws Throwable {
		if (!_node.isClosed()) {
			log.warn("node was not closed properly. cleaning up in finalize()");
			close();
		}
		super.finalize();
	}

}
