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

import java.util.List;

import org.hibernate.CacheMode;
import org.hibernate.Criteria;
import org.hibernate.FlushMode;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.classic.Session;

import at.molindo.esi4j.rebuild.RebuildSession;

public final class HibernateRebuildSession<T> implements RebuildSession<T> {

	private final Transaction _tx;
	private final Session _session;
	private final Class<T> _type;
	private final HibernateModule _module;
	private final HibernateQueryProvider _queryProvider;

	int _next = 0;

	public HibernateRebuildSession(Class<T> type, SessionFactory sessionFactory, HibernateModule module,
			HibernateQueryProvider queryProvider) {
		if (type == null) {
			throw new NullPointerException("type");
		}
		if (sessionFactory == null) {
			throw new NullPointerException("sessionFactory");
		}
		if (module == null) {
			throw new NullPointerException("module");
		}

		_type = type;
		_module = module;
		_queryProvider = queryProvider;

		_session = sessionFactory.openSession();
		_session.setCacheMode(CacheMode.GET);
		_session.setDefaultReadOnly(true);
		_session.setFlushMode(FlushMode.MANUAL);
		_tx = _session.beginTransaction();
	}

	@Override
	public List<T> getNext(int batchSize) {
		if (_next < 0) {
			throw new IllegalStateException("session already closed");
		}

		// clear previous batch
		_session.clear();

		List<T> list;

		if (_queryProvider != null) {
			Criteria criteria = _queryProvider.createCriteria(_type, _session);
			if (criteria != null) {
				list = fetch(criteria, _next, batchSize);
			} else {
				Query query = _queryProvider.createQuery(_type, _session);
				list = fetch(query, _next, batchSize);
			}
		} else {
			// default query
			list = fetch(_session.createCriteria(_type), _next, batchSize);
		}

		_next += list.size();

		return list;
	}

	@SuppressWarnings("unchecked")
	private List<T> fetch(Criteria criteria, int first, int max) {
		return criteria.setFirstResult(first).setMaxResults(max).setCacheable(false).list();
	}

	@SuppressWarnings("unchecked")
	private List<T> fetch(Query query, int first, int max) {
		return query.setFirstResult(first).setMaxResults(max).setCacheable(false).list();
	}

	@Override
	public void close() {
		_session.clear();
		_next = -1;
		_module.unsetRebuilding(_type);
		_tx.commit();
		_session.close();
	}
}