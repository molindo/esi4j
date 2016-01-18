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

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import lombok.NonNull;

/**
 * wraps {@link Scrolling}
 *
 */
public class ScrollingIterator implements Iterator<SearchHit> {

	@NonNull
	private final Scrolling scrolling;

	private String scrollId;
	private Iterator<SearchHit> batchIterator;
	private SearchHit next;

	public ScrollingIterator(@NonNull final Scrolling scrolling) {
		this.scrolling = scrolling;

		scrollId = scrolling.startScroll().execute().actionGet().getScrollId();
		nextBatch();
	}

	private void nextBatch() {
		// get next batch
		final SearchResponse response = scrolling.scroll(scrollId).execute().actionGet();

		final String previousScrollId = scrollId;
		scrollId = response.getScrollId();

		if (!previousScrollId.equals(scrollId)) {
			// clear old scroll context
			scrolling.clear(previousScrollId).execute();
		}

		// hits iterator
		batchIterator = Arrays.asList(response.getHits().getHits()).iterator();

		// end of scrolling if no more hits
		if (batchIterator.hasNext()) {
			next = batchIterator.next();
		} else {
			next = null;
			scrolling.clear(scrollId).execute();
		}
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}

	@Override
	public SearchHit next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		try {
			return next;
		} finally {
			if (batchIterator.hasNext()) {
				next = batchIterator.next();
			} else {
				nextBatch();
			}
		}

	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
