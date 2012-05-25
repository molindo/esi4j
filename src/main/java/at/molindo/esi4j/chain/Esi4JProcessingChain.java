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

import at.molindo.esi4j.rebuild.Esi4JRebuildProcessor;

/**
 * A processing chain consists of
 * 
 * <ul> <li>an {@link Esi4JEventProcessor} that turns inserts, updates and
 * deletions of objects into tasks</li> <li>an {@link Esi4JTaskProcessor} that
 * processes generated tasks</li> <li>an {@link Esi4JRebuildProcessor} to
 * rebuild indexes</li> </ul>
 * 
 */
public interface Esi4JProcessingChain {

	Esi4JEventProcessor getEventProcessor();

	Esi4JTaskProcessor getTaksProcessor();

}
