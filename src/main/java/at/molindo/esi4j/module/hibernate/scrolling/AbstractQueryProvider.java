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

import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;

/**
 * simple helper to simplify {@link QueryProvider} (normally one method less to override)
 */
public abstract class AbstractQueryProvider implements QueryProvider {

	@Override
	public Criteria createCriteria(final Class<?> type, final Session session) {
		return null;
	}

	@Override
	public Query createQuery(final Class<?> type, final Session session) {
		return null;
	}

}
