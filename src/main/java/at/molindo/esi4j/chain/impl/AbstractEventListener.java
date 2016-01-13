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
	public void onPostInsert(final Object... objects) {
		for (final Object object : objects) {
			onPostInsert(object);
		}
	}

	@Override
	public void onPostInsert(final Iterable<Object> objects) {
		for (final Object object : objects) {
			onPostInsert(object);
		}
	}

	@Override
	public void onPostUpdate(final Object... objects) {
		for (final Object object : objects) {
			onPostUpdate(object);
		}
	}

	@Override
	public void onPostUpdate(final Iterable<Object> objects) {
		for (final Object object : objects) {
			onPostUpdate(object);
		}
	}

	@Override
	public void onPostDelete(final Object... objects) {
		for (final Object object : objects) {
			onPostDelete(object);
		}
	}

	@Override
	public void onPostDelete(final Iterable<Object> objects) {
		for (final Object object : objects) {
			onPostDelete(object);
		}
	}

}
