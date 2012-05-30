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
package at.molindo.esi4j.core.impl;

import java.util.Map.Entry;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import at.molindo.esi4j.core.Esi4JClient;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.Esi4JStore;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.utils.data.StringUtils;

public class DefaultStore implements Esi4JStore {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DefaultStore.class);

	private static final long INDEX_CREATION_TIMEOUT_SECONDS = 30;

	private final Esi4JClient _client;

	private final String _indexName;

	private Esi4JIndex _index;

	public DefaultStore(Esi4JClient client, String indexName) {
		if (client == null) {
			throw new NullPointerException("client");
		}
		if (indexName == null) {
			throw new NullPointerException("indexName");
		}
		_client = client;
		_indexName = indexName;
	}

	@Override
	public Esi4JClient getClient() {
		return _client;
	}

	@Override
	public String getIndexName() {
		return _indexName;
	}

	public Esi4JIndex getIndex() {
		return _index;
	}

	@Override
	public void setIndex(Esi4JIndex index) {
		if (index == null) {
			throw new NullPointerException("index");
		}
		if (index != _index) {
			if (_index != null) {
				throw new IllegalStateException("already assigned to another index");
			}
			init(_index = index);
		}
	}

	/**
	 * called after assigning a store to a new index
	 * 
	 * @param index
	 *            the new index (same as {@link #_index})
	 */
	protected void init(Esi4JIndex index) {
		// create index

		assertIndex(index);
		_client.addStore(this);
	}

	void assertIndex(Esi4JIndex index) {
		IndicesExistsResponse existsResponse = _client.getClient().admin().indices().prepareExists(_indexName)
				.execute().actionGet();

		if (!existsResponse.exists()) {
			// create index
			CreateIndexRequestBuilder request = _client.getClient().admin().indices().prepareCreate(_indexName);

			Settings settings = getStoreSettings(index);
			if (settings != null) {
				request.setSettings(settings);
			}

			CreateIndexResponse response = request
					.setTimeout(TimeValue.timeValueSeconds(INDEX_CREATION_TIMEOUT_SECONDS)).execute().actionGet();

			if (!response.acknowledged()) {
				log.warn("index creation not acknowledged within " + INDEX_CREATION_TIMEOUT_SECONDS + " seconds");
			}

		} else {
			// update settings
			Settings settings = getStoreSettings(index);
			if (settings != null && settings.getAsMap().size() > 0) {

				Settings storeSettings = settings;
				if (settings.get(IndexMetaData.SETTING_NUMBER_OF_SHARDS) != null) {
					// es won't allow including number_of_shards
					Builder builder = ImmutableSettings.settingsBuilder().put(settings);
					builder.remove(IndexMetaData.SETTING_NUMBER_OF_SHARDS);
					storeSettings = builder.build();
				}

				_client.getClient().admin().indices().prepareUpdateSettings(_indexName).setSettings(storeSettings)
						.execute().actionGet();

				ClusterStateResponse state = _client.getClient().admin().cluster().prepareState()
						.setFilterIndices(_indexName).setFilterAll().setFilterMetaData(false).execute().actionGet();

				Settings indexSettings = state.getState().getMetaData().getIndices().get(_indexName).getSettings();

				for (Entry<String, String> e : settings.getAsMap().entrySet()) {
					String key = e.getKey();
					String localValue = e.getValue();
					String indexValue = indexSettings.get(key.startsWith("index.") ? key : "index." + key);
					if (!StringUtils.equals(localValue, indexValue)) {
						// TODO make behavior configurable: fail or warn
						log.warn("could not update value for settings key '" + key + "' from ('" + indexValue
								+ "' to '" + localValue + "') - delete and rebuild index " + _indexName);
					}
				}

				// TODO reset previously set settings to their defaults
				// TODO try closing index to update settings
			}
		}
	}

	private Settings getStoreSettings(Esi4JIndex index) {
		Settings indexSettings = ((InternalIndex) index).getSettings();
		if (indexSettings == null) {
			return null;
		}
		ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
		for (Entry<String, String> e : indexSettings.getAsMap().entrySet()) {
			if (e.getKey().startsWith("index.")) {
				builder.put(e.getKey(), e.getValue());
			}
		}
		return builder.build();
	}

	@Override
	public <T> T execute(StoreOperation<T> operation) {
		if (_index == null) {
			throw new IllegalStateException("store not assigned to an index");
		}
		return operation.execute(_client.getClient(), _indexName);
	}

	@Override
	public void close() {
		_client.removeStore(this);
	}
}
