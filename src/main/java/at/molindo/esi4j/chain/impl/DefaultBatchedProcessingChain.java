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

import at.molindo.esi4j.chain.Esi4JBatchedEventProcessor;
import at.molindo.esi4j.chain.Esi4JBatchedProcessingChain;
import at.molindo.esi4j.chain.Esi4JTaskProcessor;
import at.molindo.esi4j.chain.Esi4JTaskSource;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.rebuild.Esi4JRebuildProcessor;
import at.molindo.esi4j.rebuild.SimpleRebuildProcessor;

public class DefaultBatchedProcessingChain implements Esi4JBatchedProcessingChain {

	private final DefaultTaskProcessor _taksProcessor;
	private final SimpleRebuildProcessor _rebuildStrategy;
	private final DefaultBatchedEventProcessor _batchedEventProcessor;

	public DefaultBatchedProcessingChain(Esi4JIndex index) {
		this(index, null);
	}

	public DefaultBatchedProcessingChain(Esi4JIndex index, Map<Class<?>, Esi4JTaskSource> taskSources) {
		_taksProcessor = new DefaultTaskProcessor(index);
		_batchedEventProcessor = new DefaultBatchedEventProcessor(_taksProcessor, taskSources);
		_rebuildStrategy = new SimpleRebuildProcessor();
	}

	@Override
	public Esi4JBatchedEventProcessor getEventProcessor() {
		return _batchedEventProcessor;
	}

	@Override
	public Esi4JTaskProcessor getTaksProcessor() {
		return _taksProcessor;
	}

	@Override
	public Esi4JRebuildProcessor getRebuildProcessor() {
		return _rebuildStrategy;
	}

}
