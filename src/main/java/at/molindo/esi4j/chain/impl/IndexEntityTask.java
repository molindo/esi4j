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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;

import at.molindo.esi4j.chain.Esi4JEntityResolver;
import at.molindo.esi4j.chain.Esi4JEntityTask;
import at.molindo.esi4j.core.Esi4JIndex.OperationHelper;
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
	public void addToBulk(BulkRequestBuilder bulk, String indexName, OperationHelper helper) {
		Object entity = getEntity();
		if (entity != null) {
			TypeMapping mapping = helper.findTypeMapping(entity);

			String id = mapping.getIdString(entity);
			if (!mapping.isFiltered(entity)) {
				String type = mapping.getTypeAlias();

				Long version = mapping.getVersion(entity);

				IndexRequest request = new IndexRequest(indexName, type, id);

				if (version != null) {
					request.version(version).versionType(VersionType.EXTERNAL);
				}

				mapping.getObjectSource(entity).setSource(request);

				bulk.add(request);
			} else if (id != null) {
				bulk.add(new DeleteRequest(indexName, mapping.getTypeAlias(), id));
			}
		}
	}

}
