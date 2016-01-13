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
package at.molindo.esi4j.module;

import at.molindo.esi4j.rebuild.Esi4JRebuildProcessor;
import at.molindo.esi4j.rebuild.Esi4JRebuildSession;

/**
 * A module allows rebuilding indexes from external data sources.
 */
public interface Esi4JModule {

	/**
	 * must be called by {@link Esi4JRebuildProcessor} implementation before any modifications to underlying index
	 * occur. This way, a module might decide to queue or discard write operations until
	 * {@link Esi4JRebuildSession#close() is called}
	 *
	 * @param type
	 * @return an iterator over this module's data for the given type.
	 */
	Esi4JRebuildSession startRebuildSession(Class<?> type);

	/**
	 * @return the types this module supports.
	 */
	Class<?>[] getTypes();

	void close();

}
