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
package at.molindo.esi4j.operation;

import org.elasticsearch.client.Client;

import at.molindo.esi4j.chain.impl.SerializableEsi4JOperation;

public final class RefreshOperation implements SerializableEsi4JOperation<Void> {
	private static final long serialVersionUID = 1L;

	@Override
	public Void execute(Client client, String indexName, OperationContext helper) {
		client.admin().indices().prepareRefresh(indexName).execute().actionGet();
		return null;
	}
}