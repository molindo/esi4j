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
package at.molindo.esi4j.chain.impl;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;

import at.molindo.esi4j.chain.Esi4JEntityResolver;
import at.molindo.esi4j.chain.Esi4JEntityResolver.ObjectKey;
import at.molindo.esi4j.chain.Esi4JEntityTask;
import at.molindo.esi4j.core.Esi4JIndex.OperationHelper;
import at.molindo.esi4j.mapping.TypeMapping;

public final class DeleteEntityTask extends AbstractEntityTask {

	private static final long serialVersionUID = 1L;

	public DeleteEntityTask(Object entity) {
		super(entity);
	}

	@Override
	protected void initClone(Esi4JEntityTask clone) {
	}

	@Override
	public void replaceEntity(Esi4JEntityResolver entityResolver) {
		// always use ObjectKey
		setEntity(entityResolver.toObjectKey(getEntity()));
	}

	@Override
	public void resolveEntity(Esi4JEntityResolver entityResolver) {
		// noop - already deleted object, ObjectKey is sufficient
	}

	@Override
	public void addToBulk(BulkRequestBuilder bulk, String indexName, OperationHelper helper) {
		Object entity = getEntity();

		TypeMapping mapping;

		String id;
		if (entity instanceof ObjectKey) {
			ObjectKey key = (ObjectKey) entity;
			mapping = helper.findTypeMapping(key.getType());
			id = mapping.toIdString(key.getId());
		} else {
			mapping = helper.findTypeMapping(entity);
			id = mapping.getIdString(entity);
		}

		bulk.add(new DeleteRequest(indexName, mapping.getTypeAlias(), id));
	}

}