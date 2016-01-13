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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.update.UpdateRequest;
import org.junit.Test;

import at.molindo.esi4j.chain.Esi4JEntityResolver;
import at.molindo.esi4j.chain.Esi4JEntityTask;
import at.molindo.esi4j.chain.impl.QueuedTaskExecutor.ObjectKeyListMap;
import at.molindo.esi4j.ex.EntityNotResolveableException;
import at.molindo.esi4j.mapping.ObjectKey;
import at.molindo.utils.collections.CollectionUtils;

public class QueuedTaskExecutorTest {

	@Test
	public void resolveDuplicates() {

		final Esi4JEntityTask[] tasks = new TasksBuilder().index("foo").update("foo").delete("foo").tasks();
		assertEquals(3, tasks.length);
		assertNotNull(tasks[0]);
		assertNotNull(tasks[1]);
		assertNotNull(tasks[2]);

		final QueuedTaskExecutor.ObjectKeyListMap map = toMap(tasks);
		assertEquals(1, map.size());
		assertEquals(3, CollectionUtils.firstValue(map).size());

		QueuedTaskExecutor.resolveDuplicates(tasks, map);

		assertEquals(1, CollectionUtils.firstValue(map).size());

		assertNull(tasks[0]);
		assertNull(tasks[1]);
		assertNotNull(tasks[2]);
	}

	private ObjectKeyListMap toMap(final Esi4JEntityTask[] tasks) {
		final ObjectKeyListMap map = new QueuedTaskExecutor.ObjectKeyListMap(tasks.length);

		final StringEntityResolver resolver = new StringEntityResolver();
		for (int i = 0; i < tasks.length; i++) {
			final Esi4JEntityTask task = tasks[i];
			map.add(task.toObjectKey(resolver), i);
		}

		return map;
	}

	private static class StringEntityResolver implements Esi4JEntityResolver {

		@Override
		public ObjectKey toObjectKey(final Object entity) {
			return new ObjectKey(String.class, (String) entity);
		}

		@Override
		public Object replaceEntity(final Object entity) {
			return toObjectKey(entity);
		}

		@Override
		public Object resolveEntity(final Object replacedEntity) throws EntityNotResolveableException {
			if (replacedEntity instanceof String) {
				return replacedEntity;
			} else {
				return ((ObjectKey) replacedEntity).getId();
			}
		}

	}

	private static class TasksBuilder {
		private final List<Esi4JEntityTask> _tasks = new ArrayList<>();

		public TasksBuilder index(final String entity) {
			_tasks.add(new IndexEntityTask(entity));
			return this;
		}

		public TasksBuilder delete(final String entity) {
			_tasks.add(new DeleteEntityTask(entity));
			return this;
		}

		public TasksBuilder update(final String entity) {
			_tasks.add(new UpdateEntityTask(entity) {

				private static final long serialVersionUID = 1L;

				@Override
				protected UpdateRequest updateRequest(final Object entity) {
					return null;
				}
			});
			return this;
		}

		public Esi4JEntityTask[] tasks() {
			return _tasks.toArray(new Esi4JEntityTask[_tasks.size()]);
		}
	}
}
