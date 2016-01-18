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
package at.molindo.esi4j.rebuild.util;

import static at.molindo.utils.collections.ArrayUtils.*;

import java.util.Arrays;
import java.util.Iterator;

import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(prefix = "", chain = true, fluent = true)
public class Scrolling implements Iterable<SearchHit> {

	private static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueMinutes(10);
	private static final int DFEAULT_BATCH_SIZE = 100_000;

	private final Client client;
	private final String[] indices;

	private String[] types;
	private String[] fields;

	private boolean version = true;
	private boolean fetchSource = true;
	private boolean explain = false;

	@NonNull
	private TimeValue keepAlive = DEFAULT_KEEP_ALIVE;
	private int batchSize = DFEAULT_BATCH_SIZE;

	@NonNull
	private QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();

	public Scrolling(@NonNull final Client client, final String... indices) {
		this.client = client;
		this.indices = indices;
	}

	public Scrolling types(final String... types) {
		this.types = empty(types) ? null : types;
		return this;
	}

	public Scrolling type(final String type) {
		if (type == null) {
			return types();
		} else {
			return types(type);
		}
	}

	/**
	 * create a new scrolling context
	 */
	public SearchRequestBuilder startScroll() {
		final SearchRequestBuilder builder = client.prepareSearch(indices);
		builder.setSearchType(SearchType.SCAN);
		builder.setExplain(explain);
		builder.setFetchSource(fetchSource);
		if (empty(fields)) {
			builder.setNoFields();
		} else {
			builder.addFields(fields);
		}
		builder.setVersion(version);
		builder.setQuery(queryBuilder);
		builder.setSize(batchSize);
		builder.setTypes(types);
		builder.setScroll(keepAlive);
		return builder;
	}

	public SearchScrollRequestBuilder scroll(final String scrollId) {
		return client.prepareSearchScroll(scrollId).setScroll(keepAlive);
	}

	public ClearScrollRequestBuilder clear(final String scrollId) {
		return client.prepareClearScroll().setScrollIds(Arrays.asList(scrollId));
	}

	@Override
	public Iterator<SearchHit> iterator() {
		return new ScrollingIterator(this);
	}

}
