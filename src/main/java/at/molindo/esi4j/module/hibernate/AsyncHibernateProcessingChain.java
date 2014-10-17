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
package at.molindo.esi4j.module.hibernate;

import java.util.Map;

import at.molindo.esi4j.chain.Esi4JBatchedEventProcessor;
import at.molindo.esi4j.chain.Esi4JBatchedProcessingChain;
import at.molindo.esi4j.chain.Esi4JTaskProcessor;
import at.molindo.esi4j.chain.Esi4JTaskSource;
import at.molindo.esi4j.chain.impl.DefaultBatchedEventProcessor;
import at.molindo.esi4j.chain.impl.QueuedTaskProcessor;
import at.molindo.esi4j.core.Esi4JIndex;

public class AsyncHibernateProcessingChain implements Esi4JBatchedProcessingChain {

	private final QueuedTaskProcessor _taksProcessor;
	private final DefaultBatchedEventProcessor _batchedEventProcessor;

	public AsyncHibernateProcessingChain(Esi4JIndex index, HibernateEntityResolver entityResolver) {
		this(index, entityResolver, null);
	}

	public AsyncHibernateProcessingChain(Esi4JIndex index, HibernateEntityResolver entityResolver,
			Map<Class<?>, Esi4JTaskSource> taskSources) {

		if (entityResolver == null) {
			throw new NullPointerException("entityResolver");
		}

		_taksProcessor = new QueuedTaskProcessor(index, entityResolver);

		_batchedEventProcessor = new DefaultBatchedEventProcessor(_taksProcessor, taskSources);
	}

	@Override
	public Esi4JBatchedEventProcessor getEventProcessor() {
		return _batchedEventProcessor;
	}

	@Override
	public Esi4JTaskProcessor getTaksProcessor() {
		return _taksProcessor;
	}

}
