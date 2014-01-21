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
package at.molindo.esi4j.module.es;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import at.molindo.esi4j.core.Esi4JOperation;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.module.Esi4JModule;
import at.molindo.esi4j.rebuild.Esi4JRebuildSession;

public class ElasticsearchModule implements Esi4JModule {

	private final TransportClient _client;
	private final InternalIndex _index;

	public ElasticsearchModule(Settings transportClientSettings, InternalIndex index) {
		if (transportClientSettings == null) {
			throw new NullPointerException("transportClientSettings");
		}
		if (index == null) {
			throw new NullPointerException("index");
		}
		_client = new TransportClient(transportClientSettings);
		_index = index;
	}

	@Override
	public Esi4JRebuildSession startRebuildSession(final Class<?> type) {
		return _index.execute(new Esi4JOperation<Esi4JRebuildSession>() {

			@Override
			public Esi4JRebuildSession execute(Client client, String indexName, OperationContext helper) {
				final TypeMapping mapping = helper.findTypeMapping(type);

				return new Esi4JRebuildSession() {

					private final long _scrollTimeoutSeconds = 60;

					private String _scrollId;
					private boolean _endReached = false;

					@Override
					public boolean isOrdered() {
						return true;
					}

					@Override
					public Class<?> getType() {
						return type;
					}

					/**
					 * only uses the batchSize from the first invocation and
					 * ignores changes
					 */
					@Override
					public List<?> getNext(int batchSize) {
						if (_endReached) {
							// TODO improve
							throw new IllegalStateException("reached end");
						}

						ActionFuture<SearchResponse> responseFuture;
						if (_scrollId == null) {
							// first request
							SearchRequestBuilder builder = new SearchRequestBuilder(_client)
									.setIndices(mapping.getTypeAlias()).setFilter(FilterBuilders.matchAllFilter())
									.setScroll(TimeValue.timeValueSeconds(_scrollTimeoutSeconds))
									.addSort("_id", SortOrder.ASC).setSize(batchSize);

							responseFuture = _client.search(builder.request());
						} else {
							responseFuture = _client.searchScroll(new SearchScrollRequestBuilder(_client, _scrollId)
									.request());
						}

						SearchResponse response = responseFuture.actionGet();

						SearchHit[] hits = response.getHits().getHits();
						ArrayList<Object> list = new ArrayList<Object>(hits.length);

						if (hits.length == 0) {
							_endReached = true;
							_scrollId = null;
						} else {
							_scrollId = response.getScrollId();
							for (int i = 0; i < hits.length; i++) {
								Object o = mapping.read(hits[i]);
								if (o != null) {
									list.add(o);
								}
							}
						}

						return list;
					}

					@Override
					public Object getMetadata() {
						return null;
					}

					@Override
					public void close() {
					}
				};
			}

		});
	}

	@Override
	public Class<?>[] getTypes() {
		return _index.getMappedTypes();
	}

	@Override
	public void close() {
		_client.close();
	}

}
