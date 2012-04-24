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
package at.molindo.esi4j.chain.impl;

import at.molindo.esi4j.chain.Esi4JEventListener;

public abstract class AbstractEventListener implements Esi4JEventListener {

	@Override
	public void onPostInsert(Object... objects) {
		for (int i = 0; i < objects.length; i++) {
			onPostInsert(objects[i]);
		}
	}

	@Override
	public void onPostInsert(Iterable<Object> objects) {
		for (Object object : objects) {
			onPostInsert(object);
		}
	}

	@Override
	public void onPostUpdate(Object... objects) {
		for (int i = 0; i < objects.length; i++) {
			onPostUpdate(objects[i]);
		}
	}

	@Override
	public void onPostUpdate(Iterable<Object> objects) {
		for (Object object : objects) {
			onPostUpdate(object);
		}
	}

	@Override
	public void onPostDelete(Object... objects) {
		for (int i = 0; i < objects.length; i++) {
			onPostDelete(objects[i]);
		}
	}

	@Override
	public void onPostDelete(Iterable<Object> objects) {
		for (Object object : objects) {
			onPostDelete(object);
		}
	}

}
