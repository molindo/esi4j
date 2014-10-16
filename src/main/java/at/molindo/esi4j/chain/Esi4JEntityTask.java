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

import java.io.Serializable;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;

import at.molindo.esi4j.core.Esi4JOperation.OperationContext;
import at.molindo.esi4j.ex.EntityNotResolveableException;
import at.molindo.esi4j.mapping.ObjectKey;

/**
 * A task to be processed by an {@link Esi4JTaskProcessor}. A task needs to be
 * {@link Serializable} after {@link #replaceEntity(Esi4JEntityResolver)} was
 * called
 */
public interface Esi4JEntityTask extends Serializable, Cloneable {

	/**
	 * optional, might be implemented as noop
	 */
	void replaceEntity(Esi4JEntityResolver entityResolver);

	/**
	 * optional, might be implemented as noop if
	 * {@link #replaceEntity(Esi4JEntityResolver)} is a noop or entity is a
	 * {@link ObjectKey}
	 */
	void resolveEntity(Esi4JEntityResolver entityResolver) throws EntityNotResolveableException;

	/**
	 * returns the entities {@link ObjectKey}
	 */
	ObjectKey toObjectKey(Esi4JEntityResolver entityResolver);

	/**
	 * add necessary index operations to bulk request
	 */
	void addToBulk(Client client, BulkRequestBuilder bulk, String indexName, OperationContext context);

	/**
	 * @return a clone of this task
	 */
	Esi4JEntityTask clone();

}
