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

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;

public class CustomQueryScrollingSession implements ScrollingSession {

	private final QueryProvider _queryProvider;

	private int _next = 0;

	private final Class<?> _type;

	public CustomQueryScrollingSession(final Class<?> type, final QueryProvider queryProvider) {
		if (type == null) {
			throw new NullPointerException("type");
		}
		if (queryProvider == null) {
			throw new NullPointerException("queryProvider");
		}
		_type = type;
		_queryProvider = queryProvider;
	}

	@Override
	public boolean isOrdered() {
		return _queryProvider.isOrdered();
	}

	@Override
	public List<?> fetch(final Session session, final int batchSize) {
		List<?> list;

		final Criteria criteria = _queryProvider.createCriteria(_type, session);
		if (criteria != null) {
			list = fetch(criteria, _next, batchSize);
		} else {
			final Query query = _queryProvider.createQuery(_type, session);
			list = fetch(query, _next, batchSize);
		}

		_next += list.size();

		return list;
	}

	private List<?> fetch(final Criteria criteria, final int first, final int max) {
		// TODO there are better ways to scroll than setFirstResult(..)
		return criteria.setFirstResult(first).setMaxResults(max).setCacheable(false).list();
	}

	private List<?> fetch(final Query query, final int first, final int max) {
		// TODO there are better ways to scroll than setFirstResult(..)
		return query.setFirstResult(first).setMaxResults(max).setCacheable(false).list();
	}

}
