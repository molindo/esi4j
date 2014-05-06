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
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import at.molindo.esi4j.module.hibernate.scrolling.ScrollingSession;
import at.molindo.esi4j.rebuild.Esi4JRebuildSession;

public final class HibernateRebuildSession implements Esi4JRebuildSession {

	private final Transaction _tx;
	private final Session _session;
	private final Class<?> _type;
	private final HibernateModule _module;
	private final ScrollingSession _scrollingSession;

	public HibernateRebuildSession(Class<?> type, SessionFactory sessionFactory, HibernateModule module,
			ScrollingSession scrollingSession) {
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
		_scrollingSession = scrollingSession;

		_session = sessionFactory.openSession();
		_session.setCacheMode(CacheMode.GET);
		_session.setDefaultReadOnly(true);
		_session.setFlushMode(FlushMode.MANUAL);
		_tx = _session.beginTransaction();
	}

	@Override
	public boolean isOrdered() {
		return _scrollingSession.isOrdered();
	}

	@Override
	public Class<?> getType() {
		return _type;
	}

	@Override
	public List<?> getNext(int batchSize) {
		// clear previous batch
		_session.clear();
		return _scrollingSession.fetch(_session, batchSize);
	}

	@Override
	public Object getMetadata() {
		return null;
	}

	@Override
	public void close() {
		_session.clear();
		_module.unsetRebuilding(_type);
		_tx.commit();
		_session.close();
	}

}