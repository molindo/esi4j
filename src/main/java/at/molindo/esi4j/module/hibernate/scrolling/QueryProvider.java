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

public interface QueryProvider {

	/**
	 * create a criteria for bulk indexing of type
	 */
	Criteria createCriteria(Class<?> type, Session session);

	/**
	 * fallback if {@link #createCriteria(Session)} returns null
	 */
	Query createQuery(Class<?> type, Session session);
}
