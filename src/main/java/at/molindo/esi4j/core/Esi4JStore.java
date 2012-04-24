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
package at.molindo.esi4j.core;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.elasticsearch.client.Client;

public interface Esi4JStore {

	/**
	 * elasticsearch index name (must not change)
	 */
	@Nonnull
	String getIndexName();

	/**
	 * the esi4j index currently using this store (must not change)
	 */
	@CheckForNull
	Esi4JIndex getIndex();

	<T> T execute(StoreOperation<T> operation);

	/**
	 * @param index
	 * @throws IllegalStateException
	 *             if store was already assigned to an index
	 */
	void setIndex(Esi4JIndex index);

	Esi4JClient getClient();

	void close();

	public interface StoreOperation<T> {

		/**
		 * executes a given operation against an index. might be called multiple
		 * times and even concurrently against different indexes or clusters. In
		 * such cases, the result returned by the main cluster and index must be
		 * returned while the {@link Esi4JStore} must handle different
		 * responses.
		 * 
		 * TODO how's that supposed to work?
		 * 
		 * @param client
		 *            the elasticsearch client to use
		 * @param indexName
		 *            the elasticsearch index to use
		 * 
		 * @return response by main index
		 */
		T execute(Client client, String indexName);

	}
}
