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

import java.util.Map;

import at.molindo.esi4j.chain.Esi4JEntityTask;
import at.molindo.esi4j.chain.Esi4JEventProcessor;
import at.molindo.esi4j.chain.Esi4JTaskProcessor;
import at.molindo.esi4j.chain.Esi4JTaskSource;
import at.molindo.utils.collections.ArrayUtils;
import at.molindo.utils.collections.ClassMap;
import at.molindo.utils.collections.CollectionUtils;

public class DefaultEventProcessor extends AbstractEventListener implements Esi4JEventProcessor {

	private volatile ClassMap<Esi4JTaskSource> _taskSources;
	private final Esi4JTaskProcessor _taskProcessor;

	public DefaultEventProcessor(Esi4JTaskProcessor taskProcessor) {
		this(taskProcessor, null);
	}

	public DefaultEventProcessor(Esi4JTaskProcessor taskProcessor, Map<Class<?>, Esi4JTaskSource> taskSources) {
		if (taskProcessor == null) {
			throw new NullPointerException("taskProcessor");
		}
		_taskProcessor = taskProcessor;
		_taskSources = new ClassMap<Esi4JTaskSource>();

		if (!CollectionUtils.empty(taskSources)) {
			_taskSources.putAll(taskSources);
		}
	}

	@Override
	public boolean isProcessing(Class<?> type) {
		return _taskSources.find(type) != null;
	}

	@Override
	public void putTaskSource(Class<?> type, Esi4JTaskSource taskSource) {
		// copy on write
		ClassMap<Esi4JTaskSource> temp = copyTaskSources();
		temp.put(type, taskSource);
		_taskSources = temp;
	}

	@Override
	public void removeTaskSource(Class<?> type) {
		// copy on write
		ClassMap<Esi4JTaskSource> temp = copyTaskSources();
		temp.remove(type);
		_taskSources = temp;
	}

	ClassMap<Esi4JTaskSource> copyTaskSources() {
		return new ClassMap<Esi4JTaskSource>(_taskSources);
	}

	protected void processTasks(Esi4JEntityTask[] tasks) {
		if (!ArrayUtils.empty(tasks)) {
			_taskProcessor.processTasks(tasks);
		}
	}

	protected Esi4JTaskSource findTaskSource(Object o) {
		return o == null ? null : _taskSources.find(o.getClass());
	}

	@Override
	public void onPostInsert(Object o) {
		processTasks(getPostInsertTasks(o));
	}

	@Override
	public void onPostUpdate(Object o) {
		processTasks(getPostUpdateTasks(o));
	}

	@Override
	public void onPostDelete(Object o) {
		processTasks(getPostDeleteTasks(o));
	}

	protected Esi4JEntityTask[] getPostInsertTasks(Object o) {
		Esi4JTaskSource src = findTaskSource(o);
		return src == null ? null : src.getPostInsertTasks(o);
	}

	protected Esi4JEntityTask[] getPostUpdateTasks(Object o) {
		Esi4JTaskSource src = findTaskSource(o);
		return src == null ? null : src.getPostUpdateTasks(o);
	}

	protected Esi4JEntityTask[] getPostDeleteTasks(Object o) {
		Esi4JTaskSource src = findTaskSource(o);
		return src == null ? null : src.getPostDeleteTasks(o);
	}

	@Override
	public Esi4JTaskProcessor getTaskProcessor() {
		return _taskProcessor;
	}

	@Override
	public void close() {
	}

}
