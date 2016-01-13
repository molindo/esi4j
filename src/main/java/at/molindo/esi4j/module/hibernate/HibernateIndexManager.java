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
import at.molindo.esi4j.module.hibernate.scrolling.CustomQueryScrollingSessionProvider;
import at.molindo.esi4j.module.hibernate.scrolling.QueryProvider;
import at.molindo.esi4j.module.hibernate.scrolling.ScrollingSessionProvider;
import at.molindo.esi4j.module.hibernate.scrolling.SimpleQueryProvider;
import at.molindo.esi4j.rebuild.Esi4JRebuildManager;
import at.molindo.esi4j.rebuild.impl.DefaultRebuildManager;

public class HibernateIndexManager extends AbstractIndexManager {

	private final HibernateLifecycleInjector _lifecycleInjector;

	public HibernateIndexManager(final SessionFactory sessionFactory, final Esi4JIndex index, final Esi4JBatchedProcessingChain processingChain) {
		this(sessionFactory, index, processingChain, new DefaultRebuildManager());
	}

	public HibernateIndexManager(final SessionFactory sessionFactory, final Esi4JIndex index, final Esi4JBatchedProcessingChain processingChain, final Esi4JRebuildManager rebuildManager) {
		super(new HibernateModule(sessionFactory), (InternalIndex) index, processingChain, rebuildManager);

		// inject listener into session factory and start processing
		_lifecycleInjector = new DefaultHibernateLifecycleInjector(false);
		_lifecycleInjector.injectLifecycle(sessionFactory, getBatchedProcessingChain().getEventProcessor());
	}

	@Override
	protected void onBeforeClose() {
		final SessionFactory sessionFactory = getHibernateModule().getSessionFactory();
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

	/**
	 * @param type
	 * @param hql
	 * @param ordered
	 *            results ordered by id
	 * @return this
	 */
	public HibernateIndexManager query(final Class<?> type, final String hql, final boolean ordered) {
		return queryProvider(type, new SimpleQueryProvider(hql, ordered));
	}

	/**
	 * @param type
	 * @param criteria
	 * @param ordered
	 *            results ordered by id
	 * @return this
	 */
	public HibernateIndexManager criteria(final Class<?> type, final DetachedCriteria criteria, final boolean ordered) {
		return queryProvider(type, new SimpleQueryProvider(criteria, ordered));
	}

	/**
	 * @param type
	 * @param queryProvider
	 * @return this
	 */
	public HibernateIndexManager queryProvider(final Class<?> type, final QueryProvider queryProvider) {
		scrollingProvider(new CustomQueryScrollingSessionProvider(type, queryProvider));
		return this;
	}

	public HibernateIndexManager scrollingProvider(final ScrollingSessionProvider scrollingProvider) {
		getHibernateModule().putScrollingProvider(scrollingProvider);
		return this;
	}
}
