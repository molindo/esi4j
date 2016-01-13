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
package at.molindo.esi4j.module.hibernate.scrolling;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.FetchMode;
import org.hibernate.Session;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.metadata.ClassMetadata;

public class DefaultQueryScrollingSession implements ScrollingSession {

	private final Class<?> _type;
	private Serializable _lastId;
	private Map<String, FetchMode> _fetchModes;

	public DefaultQueryScrollingSession(final Class<?> type) {
		if (type == null) {
			throw new NullPointerException("type");
		}
		_type = type;
		_fetchModes = Collections.emptyMap();
	}

	public DefaultQueryScrollingSession(final Class<?> type, final Map<String, FetchMode> fetchModes) {
		this(type);
		_fetchModes = new HashMap<String, FetchMode>(fetchModes);
	}

	@Override
	public boolean isOrdered() {
		return true;
	}

	@Override
	public List<?> fetch(final Session session, final int batchSize) {
		final Criteria criteria = session.createCriteria(_type);
		if (_lastId != null) {
			criteria.add(Restrictions.gt("id", _lastId));
		}
		criteria.addOrder(Order.asc("id"));
		criteria.setMaxResults(batchSize);
		criteria.setCacheable(false);

		for (final Map.Entry<String, FetchMode> e : _fetchModes.entrySet()) {
			criteria.setFetchMode(e.getKey(), e.getValue());
		}

		final List<?> list = criteria.list();

		if (list.size() > 0) {
			final ClassMetadata meta = session.getSessionFactory().getClassMetadata(_type);

			final Object last = list.get(list.size() - 1);
			_lastId = meta.getIdentifier(last, (SessionImplementor) session);
		}

		return list;
	}

}
