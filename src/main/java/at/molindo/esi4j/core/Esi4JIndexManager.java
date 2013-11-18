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

import at.molindo.esi4j.module.Esi4JModule;

/**
 * TODO define relationship to {@link Esi4JModule}
 */
public interface Esi4JIndexManager {

	/**
	 * @return managed index (never null, never changes)
	 */
	Esi4JManagedIndex getIndex();

	public Class<?>[] getTypes();

	void rebuild(Class<?>... types);

	/**
	 * submits a refresh operation to the underlying processing chain and waits
	 * for it to complete
	 */
	void refresh();

	void close();
}
