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
import at.molindo.esi4j.chain.Esi4JTaskSource;

public class DefaultTaskSource implements Esi4JTaskSource {

	@Override
	public Esi4JEntityTask[] getPostInsertTasks(final Object entity) {
		return new Esi4JEntityTask[] { new IndexEntityTask(entity) };
	}

	@Override
	public Esi4JEntityTask[] getPostUpdateTasks(final Object entity) {
		return getPostInsertTasks(entity);
	}

	@Override
	public Esi4JEntityTask[] getPostDeleteTasks(final Object entity) {
		return new Esi4JEntityTask[] { new DeleteEntityTask(entity) };
	}

}
