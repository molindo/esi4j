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
package at.molindo.esi4j.chain;

/**
 * processes object inserts, updates and deletions by passing them to an {@link Esi4JTaskProcessor}
 */
public interface Esi4JEventProcessor extends Esi4JEventListener {

	/**
	 * @return the {@link Esi4JTaskProcessor} processing the generated tasks
	 */
	Esi4JTaskProcessor getTaskProcessor();

	/**
	 * @return true if an {@link Esi4JTaskSource} is registered for this class or one of its supertypes
	 */
	boolean isProcessing(Class<?> type);

	/**
	 * registers a new {@link Esi4JTaskSource} for the given class
	 */
	void putTaskSource(Class<?> type, Esi4JTaskSource taskSource);

	/**
	 * remove the currently registered {@link Esi4JTaskSource} for the given class
	 */
	void removeTaskSource(Class<?> type);

	void close();

}
