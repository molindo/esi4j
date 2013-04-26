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
package at.molindo.esi4j.rebuild.scrutineer;

import org.elasticsearch.client.Client;

import at.molindo.esi4j.core.Esi4JOperation.OperationContext;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.rebuild.util.BulkIndexHelper;
import at.molindo.esi4j.rebuild.util.BulkIndexHelper.Session;

import com.aconex.scrutineer.IdAndVersion;
import com.aconex.scrutineer.IdAndVersionStreamVerifierListener;

/**
 * primary stream is module, secondary is index
 */
public class VerifierListener implements IdAndVersionStreamVerifierListener {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(VerifierListener.class);

	private final TypeMapping _mapping;
	private final Session _bulkSession;

	private int _index;
	private int _update;
	private int _delete;

	public VerifierListener(Client client, String indexName, OperationContext context, TypeMapping mapping,
			BulkIndexHelper bulkHelper, int batchSize) {

		_mapping = mapping;
		_bulkSession = bulkHelper.newSession(client, indexName, context, batchSize);
	}

	@Override
	public void onMissingInSecondaryStream(IdAndVersion primaryItem) {
		_index++;
		index(((ObjectIdAndVersion) primaryItem));
	}

	@Override
	public void onVersionMisMatch(IdAndVersion primaryItem, IdAndVersion secondaryItem) {
		_update++;
		index(((ObjectIdAndVersion) primaryItem));
	}

	@Override
	public void onMissingInPrimaryStream(IdAndVersion secondaryItem) {
		_delete++;
		delete(secondaryItem);
	}

	private void index(ObjectIdAndVersion objectIdAndVersion) {
		_bulkSession.index(objectIdAndVersion.getObject());
	}

	private void delete(IdAndVersion idAndVersion) {
		_bulkSession.delete(_mapping.getTypeClass(), _mapping.toId(idAndVersion.getId()), idAndVersion.getVersion());
	}

	public void close() {
		_bulkSession.submit();
		log.info("submitted " + (_index + _update + _delete) + " requests (" + _index + " index, " + _update
				+ " update, " + _delete + " delete)");
	}
}