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

import org.elasticsearch.common.settings.Settings;

import at.molindo.esi4j.util.Esi4JUtils;

/**
 * a generic factory that allows to create instances. Implementors must either
 * have a default constructor or a constructor accepting {@link Settings}
 * 
 * @see Esi4JUtils#createObject(Class,
 *      org.elasticsearch.common.settings.Settings)
 * 
 * @param <T>
 *            type of returned objects
 */
public interface Esi4JFactory<T> {

	/**
	 * @return a new instance of T
	 */
	T create();
}
