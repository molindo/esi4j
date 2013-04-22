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

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.rebuild.util.BulkIndexHelper;

import com.aconex.scrutineer.IdAndVersion;
import com.aconex.scrutineer.IdAndVersionStreamVerifierListener;

/**
 * primary stream is module, secondary is index
 */
public class VerifierListener implements IdAndVersionStreamVerifierListener {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(VerifierListener.class);

	private final Client _client;
	private final String _indexName;
	private final TypeMapping _mapping;
	private final int _batchSize;
	private final BulkIndexHelper _helper;

	private BulkRequestBuilder _bulkRequest;

	private int _index;
	private int _update;
	private int _delete;

	public VerifierListener(Client client, String indexName, TypeMapping mapping, BulkIndexHelper helper, int batchSize) {
		_client = client;
		_indexName = indexName;
		_mapping = mapping;
		_helper = helper;
		_batchSize = batchSize;
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
		add(_mapping.indexRequest(_client, _indexName, objectIdAndVersion.getObject()));
	}

	private void delete(IdAndVersion idAndVersion) {
		add(_mapping.deleteRequest(_client, _indexName, idAndVersion.getId(), idAndVersion.getVersion()));
	}

	private void add(ActionRequestBuilder<?, ?, ?> request) {
		if (request == null) {
			throw new IllegalArgumentException("stream contained object without id or filtered object");
		}

		if (_bulkRequest == null) {
			_bulkRequest = _client.prepareBulk();
		}

		if (request instanceof DeleteRequestBuilder) {
			_bulkRequest.add((DeleteRequestBuilder) request);
		} else {
			_bulkRequest.add((IndexRequestBuilder) request);
		}

		if (_bulkRequest.numberOfActions() >= _batchSize) {
			submitRequest();
		}
	}

	private void submitRequest() {
		_helper.bulkIndex(_bulkRequest);
		_bulkRequest = null;
	}

	public void close() {
		if (_bulkRequest != null) {
			submitRequest();
		}
		log.info("submitted " + (_index + _update + _delete) + " requests (" + _index + " index, " + _update
				+ " update, " + _delete + " delete)");
	}

}