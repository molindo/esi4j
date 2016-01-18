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

import java.util.Iterator;

import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;

import at.molindo.esi4j.rebuild.util.Scrolling;
import at.molindo.esi4j.rebuild.util.ScrollingIterator;
import at.molindo.scrutineer.IdAndVersion;
import at.molindo.scrutineer.IdAndVersionFactory;
import at.molindo.scrutineer.IdAndVersionStream;
import at.molindo.utils.collections.IteratorUtils;
import at.molindo.utils.data.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * wraps a {@link Scrolling} result transforming {@link SearchHit search hits} into {@link IdAndVersion}
 */
@RequiredArgsConstructor
public class ElasticsearchIdAndVersionStream implements IdAndVersionStream {

	static final int BATCH_SIZE = 100000;
	static final int SCROLL_TIME_IN_MINUTES = 10;

	@NonNull
	private final Client client;

	@NonNull
	private final String indexName;

	@NonNull
	private final String type;

	@NonNull
	private final IdAndVersionFactory idAndVersionFactory;

	@Override
	public boolean isSorted() {
		return false;
	}

	@Override
	public void open() {
	}

	@Override
	public Iterator<IdAndVersion> iterator() {
		return IteratorUtils.transform(new ScrollingIterator(new Scrolling(client, indexName).type(type)
				.fetchSource(false).version(true)), new Function<SearchHit, IdAndVersion>() {

					@Override
					public IdAndVersion apply(final SearchHit hit) {
						return idAndVersionFactory.create(hit.getId(), hit.getVersion());
					}
				});
	}

	@Override
	public void close() {
	}

}
