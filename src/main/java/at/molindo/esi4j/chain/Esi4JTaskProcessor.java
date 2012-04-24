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

import at.molindo.esi4j.core.Esi4JIndex;

/**
 * processes {@link Esi4JEntityTask}s against an {@link Esi4JIndex}. For
 * instance, a processor might be synchronous or asynchronous.
 */
public interface Esi4JTaskProcessor {

	/**
	 * @return an index, must never change
	 */
	Esi4JIndex getIndex();

	void processTasks(Esi4JEntityTask[] tasks);

	/**
	 * same as calling {@link #processTasks(Esi4JEntityTask[])} multiple times,
	 * but might have better performance
	 */
	void processTasks(Iterable<Esi4JEntityTask[]> tasks);

	void close();

}
