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

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import at.molindo.esi4j.chain.Esi4JBatchedEventProcessor;
import at.molindo.esi4j.chain.Esi4JEntityTask;
import at.molindo.esi4j.chain.Esi4JTaskProcessor;
import at.molindo.esi4j.chain.Esi4JTaskSource;
import at.molindo.utils.collections.ArrayUtils;

public class DefaultBatchedEventProcessor extends DefaultEventProcessor implements Esi4JBatchedEventProcessor {

	public DefaultBatchedEventProcessor(final Esi4JTaskProcessor taskProcessor) {
		super(taskProcessor);
	}

	public DefaultBatchedEventProcessor(final Esi4JTaskProcessor taskProcessor, final Map<Class<?>, Esi4JTaskSource> taskSources) {
		super(taskProcessor, taskSources);
	}

	@Override
	public EventSession startSession() {
		return new SimpleEventSession();
	}

	public class SimpleEventSession extends AbstractEventListener implements EventSession {

		private final List<Esi4JEntityTask[]> _tasks = Lists.newArrayList();
		private int _taskCount = 0;

		@Override
		public void onPostInsert(final Object o) {
			addTasks(getPostInsertTasks(o));
		}

		@Override
		public void onPostUpdate(final Object o) {
			addTasks(getPostUpdateTasks(o));
		}

		@Override
		public void onPostDelete(final Object o) {
			addTasks(getPostDeleteTasks(o));
		}

		private void addTasks(final Esi4JEntityTask[] tasks) {
			if (!ArrayUtils.empty(tasks)) {
				_tasks.add(tasks);
				_taskCount += tasks.length;
			}
		}

		@Override
		public void flush() {
			final Esi4JEntityTask[] allTasks = new Esi4JEntityTask[_taskCount];
			int from = 0;
			for (final Esi4JEntityTask[] tasks : _tasks) {
				System.arraycopy(tasks, 0, allTasks, from, tasks.length);
				from += tasks.length;
			}

			_tasks.clear();
			_taskCount = 0;

			/*
			 * important: process all tasks in a single batch so we can later identify duplicates
			 */
			processTasks(allTasks);
		}

	}
}
