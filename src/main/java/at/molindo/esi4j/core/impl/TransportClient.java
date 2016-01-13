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

import org.elasticsearch.client.Client;

import at.molindo.esi4j.core.Esi4JClient;
import at.molindo.esi4j.core.Esi4JStore;
import at.molindo.utils.collections.IdentityHashSet;

public class TransportClient implements Esi4JClient {

	private final String _id;
	private final Client _client;
	private final IdentityHashSet<Esi4JStore> _referrers = new IdentityHashSet<Esi4JStore>();

	public TransportClient(final String clusterName, final Client client) {
		if (client == null) {
			throw new NullPointerException("node");
		}
		_id = clusterName;
		_client = client;
	}

	@Override
	public String getClusterName() {
		return _id;
	}

	@Override
	public Client getClient() {
		return _client;
	}

	@Override
	public void addStore(final Esi4JStore store) {
		_referrers.add(store);
	}

	@Override
	public void removeStore(final Esi4JStore store) {
		_referrers.remove(store);
		if (_referrers.size() == 0) {
			// TODO close client?
			// _client.close();
		}
	}

	@Override
	public void close() {
		_client.close();
	}

}
