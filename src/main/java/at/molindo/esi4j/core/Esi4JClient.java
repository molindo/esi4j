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

import org.elasticsearch.client.Client;

/**
 * Manages a {@link Client}. Created by a {@link Esi4JClientFactory}. There must only be one instance per ES cluster.
 */
public interface Esi4JClient {

	public static final String DEFAULT_CLUSTER_NAME = "esi4j";

	String getClusterName();

	Client getClient();

	void addStore(Esi4JStore store);

	void removeStore(Esi4JStore store);

	void close();

}
