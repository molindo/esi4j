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
 * turns an entity into an array of {@link Esi4JEntityTask}s. For instance, an entity could cause an updated to an
 * indexed parent rather than to itself.
 */
public interface Esi4JTaskSource {
	Esi4JEntityTask[] getPostInsertTasks(Object entity);

	Esi4JEntityTask[] getPostUpdateTasks(Object entity);

	Esi4JEntityTask[] getPostDeleteTasks(Object entity);
}
