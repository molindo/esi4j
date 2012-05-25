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

import java.io.Serializable;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;
import org.hibernate.impl.SessionImpl;
import org.hibernate.metadata.ClassMetadata;

public class DefaultQueryScrolling<T> implements HibernateScrolling<T> {

	private final Class<T> _type;
	private Serializable _lastId;

	public DefaultQueryScrolling(Class<T> type) {
		if (type == null) {
			throw new NullPointerException("type");
		}
		_type = type;
	}

	@Override
	public List<T> fetch(Session session, int batchSize) {
		Criteria criteria = session.createCriteria(_type);
		if (_lastId != null) {
			criteria.add(Restrictions.gt("id", _lastId));
		}
		criteria.addOrder(Order.asc("id"));
		criteria.setMaxResults(batchSize);
		criteria.setCacheable(false);

		@SuppressWarnings("unchecked")
		List<T> list = criteria.list();

		if (list.size() > 0) {
			ClassMetadata meta = session.getSessionFactory().getClassMetadata(_type);

			T last = list.get(list.size() - 1);
			_lastId = meta.getIdentifier(last, (SessionImpl) session);
		}

		return list;
	}

}
