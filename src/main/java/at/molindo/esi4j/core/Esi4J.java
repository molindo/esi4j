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

/**
 * Facade for elasticsearch integration. Usually only one instance per
 * application.
 */
public interface Esi4J {

	/**
	 * default index name used for {@link #getIndex()}
	 */
	public static final String DEFAULT_INDEX = "default";

	/**
	 * default client name used in settings (prefix "esi4j.client." +
	 * {@value #DEFAULT_CLIENT})
	 */
	public static final String DEFAULT_CLIENT = "default";

	/**
	 * @return the default index
	 * @see #DEFAULT_INDEX
	 * @throws IllegalStateException
	 *             if default index isn't configured
	 */
	Esi4JIndex getIndex();

	/**
	 * @return the default index
	 * @throws IllegalStateException
	 *             if index with given name isn't configured
	 */
	Esi4JIndex getIndex(String name);

	void registerIndexManger(Esi4JIndexManager indexManager);

	/**
	 * close this instance an all its indexes, stores, modules, clients, ...
	 */
	void close();
}
