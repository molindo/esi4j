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

import at.molindo.esi4j.module.hibernate.HibernateIndexManager;
import at.molindo.esi4j.module.hibernate.HibernateModule;

/**
 * factory for {@link ScrollingSession} instances that are created per session.
 * There must only be one provider per type and {@link HibernateModule} (and
 * thus {@link HibernateIndexManager}). If no {@link ScrollingSessionProvider}
 * is defined for a type, a {@link DefaultQueryScrollingSession} will be used.
 */
public interface ScrollingSessionProvider {

	Class<?> getType();

	ScrollingSession newScrollingSession();
}
