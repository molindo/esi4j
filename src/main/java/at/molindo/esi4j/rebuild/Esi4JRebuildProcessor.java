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
package at.molindo.esi4j.rebuild;

import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.module.Esi4JModule;

/**
 * A strategy to rebuild the index from the given module. Different strategies
 * might include concurrency, might choose to clear and rebuild the index,
 * replace an existing index after rebuilding, etc.
 */
public interface Esi4JRebuildProcessor {

	/**
	 * rebuild all types in index using data from module
	 */
	void rebuild(Esi4JModule module, InternalIndex index, Class<?>... types);

	/**
	 * close resources
	 */
	void close();

}
