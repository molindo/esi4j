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
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;

import at.molindo.esi4j.chain.Esi4JEntityResolver;
import at.molindo.esi4j.chain.Esi4JEntityTask;
import at.molindo.esi4j.core.Esi4JOperation.OperationContext;
import at.molindo.esi4j.mapping.TypeMapping;

public final class IndexEntityTask extends AbstractEntityTask {

	private static final long serialVersionUID = 1L;

	public IndexEntityTask(Object entity) {
		super(entity);
	}

	@Override
	protected void initClone(Esi4JEntityTask clone) {
	}

	@Override
	public void replaceEntity(Esi4JEntityResolver entityResolver) {
		setEntity(entityResolver.replaceEntity(getEntity()));
	}

	@Override
	public void resolveEntity(Esi4JEntityResolver entityResolver) {
		setEntity(entityResolver.resolveEntity(getEntity()));
	}

	@Override
	public void addToBulk(BulkRequestBuilder bulk, String indexName, OperationContext context) {
		Object entity = getEntity();
		if (entity != null) {
			TypeMapping mapping = context.findTypeMapping(entity);
			IndexRequestBuilder index = mapping.indexRequest(null, indexName, entity);

			if (index != null) {
				bulk.add(index);
			} else {
				DeleteRequestBuilder delete = mapping.deleteRequest(null, indexName, entity);
				if (delete != null) {
					bulk.add(delete);
				}
			}
		}
	}

}
