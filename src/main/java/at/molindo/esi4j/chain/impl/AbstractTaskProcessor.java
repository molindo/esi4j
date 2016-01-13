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

import at.molindo.esi4j.chain.Esi4JEntityTask;
import at.molindo.esi4j.chain.Esi4JTaskProcessor;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.utils.collections.ArrayUtils;

/**
 *
 */
public abstract class AbstractTaskProcessor implements Esi4JTaskProcessor {

	private final Esi4JIndex _index;

	public AbstractTaskProcessor(final Esi4JIndex index) {
		if (index == null) {
			throw new NullPointerException("index");
		}
		_index = index;
	}

	@Override
	public void processTasks(final Iterable<Esi4JEntityTask[]> tasks) {
		if (tasks != null) {
			for (final Esi4JEntityTask[] t : tasks) {
				if (!ArrayUtils.empty(t)) {
					processTasks(t);
				}
			}
		}
	}

	@Override
	public Esi4JIndex getIndex() {
		return _index;
	}

	@Override
	public void close() {
	}

}
