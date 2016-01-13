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
package at.molindo.esi4j.chain;

import at.molindo.esi4j.module.hibernate.HibernateEntityResolver;

/**
 * an {@link Esi4JEntityResolver} that requires a session to work, e.g. {@link HibernateEntityResolver}
 */
public interface Esi4JSessionEntityResolver extends Esi4JEntityResolver {

	/**
	 * starts a session. must be called before calling {@link #resolveEntity(Object)}
	 */
	public void startResolveSession();

	/**
	 * closes the session. must be called after {@link #startResolveSession()}
	 */
	public void closeResolveSession();
}
