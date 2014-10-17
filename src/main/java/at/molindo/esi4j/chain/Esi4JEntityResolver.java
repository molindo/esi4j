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

import at.molindo.esi4j.chain.impl.QueuedTaskExecutor;
import at.molindo.esi4j.ex.EntityNotResolveableException;
import at.molindo.esi4j.mapping.ObjectKey;

/**
 * replaces objects with placeholders (e.g. for serialization or queuing) and
 * resolves them for indexing.
 * 
 * @see QueuedTaskExecutor
 */
public interface Esi4JEntityResolver {

	/**
	 * @return a new {@link ObjectKey} for this entity
	 */
	ObjectKey toObjectKey(Object entity);

	/**
	 * @return a resolveable placeholder for this entity. Might be a new
	 *         {@link ObjectKey}
	 */
	Object replaceEntity(Object entity);

	/**
	 * @return the replaced entity, the given entity if not replaced or
	 * @throws EntityNotResolveableException
	 *             if entity cannot be resolved
	 */
	Object resolveEntity(Object replacedEntity) throws EntityNotResolveableException;

}
