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
package at.molindo.esi4j.multi;

import java.util.Map;

import at.molindo.esi4j.core.Esi4JManagedIndex;
import at.molindo.esi4j.core.internal.InternalIndex;

public interface Esi4JManagedMultiIndex extends Esi4JManagedIndex {

	/**
	 * @return unmodifiable map (must not change)
	 */
	Map<String, InternalIndex> getIndices();

}
