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

import org.hibernate.SessionFactory;
import org.hibernate.criterion.DetachedCriteria;

import at.molindo.esi4j.chain.Esi4JBatchedProcessingChain;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.impl.AbstractIndexManager;
import at.molindo.esi4j.core.internal.InternalIndex;

public class HibernateIndexManager extends AbstractIndexManager {

	private final HibernateLifecycleInjector _lifecycleInjector;

	public HibernateIndexManager(SessionFactory sessionFactory, Esi4JIndex index,
			Esi4JBatchedProcessingChain processingChain) {
		super(new HibernateModule(sessionFactory), (InternalIndex) index, processingChain);

		// inject listener into session factory and start processing
		_lifecycleInjector = new DefaultHibernateLifecycleInjector(false);
		_lifecycleInjector.injectLifecycle(sessionFactory, getBatchedProcessingChain().getEventProcessor());
	}

	@Override
	protected void onBeforeClose() {
		SessionFactory sessionFactory = getHibernateModule().getSessionFactory();
		if (sessionFactory != null) {
			_lifecycleInjector.removeLifecycle(sessionFactory);
		}
	}

	private Esi4JBatchedProcessingChain getBatchedProcessingChain() {
		return (Esi4JBatchedProcessingChain) getProcessingChain();
	}

	private HibernateModule getHibernateModule() {
		return (HibernateModule) getModule();
	}

	public HibernateIndexManager query(Class<?> type, String hql) {
		return queryProvider(type, new SimpleQueryProvider(hql));
	}

	public HibernateIndexManager criteria(Class<?> type, DetachedCriteria criteria) {
		return queryProvider(type, new SimpleQueryProvider(criteria));
	}

	public HibernateIndexManager queryProvider(Class<?> type, HibernateQueryProvider queryProvider) {
		getHibernateModule().putQueryProvider(type, queryProvider);
		return this;
	}
}
