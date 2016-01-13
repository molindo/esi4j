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
 * listens for entity inserts, updates and deletions in a primary store (e.g. database)
 */
public interface Esi4JEventListener {
	void onPostInsert(Object object);

	void onPostInsert(Object... objects);

	void onPostInsert(Iterable<Object> objects);

	void onPostUpdate(Object object);

	void onPostUpdate(Object... objects);

	void onPostUpdate(Iterable<Object> objects);

	void onPostDelete(Object object);

	void onPostDelete(Object... objects);

	void onPostDelete(Iterable<Object> objects);
}
