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
import org.hibernate.FlushMode;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.classic.Session;

import at.molindo.esi4j.rebuild.Esi4JRebuildSession;

public final class HibernateRebuildSession<T> implements Esi4JRebuildSession<T> {

	private final Transaction _tx;
	private final Session _session;
	private final Class<T> _type;
	private final HibernateModule _module;
	private final HibernateScrolling<T> _scrolling;

	public HibernateRebuildSession(Class<T> type, SessionFactory sessionFactory, HibernateModule module,
			HibernateScrolling<T> scrolling) {
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
		_scrolling = scrolling;

		_session = sessionFactory.openSession();
		_session.setCacheMode(CacheMode.GET);
		_session.setDefaultReadOnly(true);
		_session.setFlushMode(FlushMode.MANUAL);
		_tx = _session.beginTransaction();
	}

	@Override
	public List<T> getNext(int batchSize) {
		// clear previous batch
		_session.clear();
		return _scrolling.fetch(_session, batchSize);
	}

	@Override
	public void close() {
		_session.clear();
		_module.unsetRebuilding(_type);
		_tx.commit();
		_session.close();
	}
}