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

import static org.junit.Assert.*;

import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import at.molindo.esi4j.core.Esi4J;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.Esi4JOperation;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.test.util.TestUtils;
import at.molindo.esi4j.test.util.Tweet;
import at.molindo.esi4j.test.util.TweetTypeMapping;

public class ScrollingTest {

	@Test
	public void test() throws Exception {
		final Esi4J esi4j = TestUtils.newEsi4j();
		final Esi4JIndex index = index(esi4j);

		index.execute(new Esi4JOperation<Void>() {

			@Override
			public Void execute(final Client client, final String indexName, final at.molindo.esi4j.core.Esi4JOperation.OperationContext helper) {
				final Scrolling scrolling = new Scrolling(client, indexName)
						.type(helper.findTypeMapping(Tweet.class).getTypeAlias());

				int c = 0;
				for (final SearchHit h : scrolling) {
					assertEquals(0, h.getFields().size());
					assertNotNull(h.getSource());
					assertNotEquals(0, h.getVersion());
					c++;
				}
				assertEquals(2, c);

				return null;
			}

		});

		esi4j.close();
	}

	@Test
	public void testNoSource() throws Exception {
		final Esi4J esi4j = TestUtils.newEsi4j();
		final Esi4JIndex index = index(esi4j);

		index.execute(new Esi4JOperation<Void>() {

			@Override
			public Void execute(final Client client, final String indexName, final at.molindo.esi4j.core.Esi4JOperation.OperationContext helper) {
				final Scrolling scrolling = new Scrolling(client, indexName)
						.type(helper.findTypeMapping(Tweet.class).getTypeAlias()).fetchSource(false).version(false)
						.batchSize(1);

				int c = 0;
				for (final SearchHit h : scrolling) {
					assertEquals(0, h.getFields().size());
					assertNull(h.getSource());
					assertEquals(-1, h.getVersion());
					c++;
				}
				assertEquals(2, c);

				return null;
			}

		});

		esi4j.close();
	}

	private static Esi4JIndex index(final Esi4J esi4j) {
		final Esi4JIndex index = esi4j.getIndex();

		((InternalIndex) index).addTypeMapping(new TweetTypeMapping("tweet"));

		final Tweet t1 = new Tweet(1, 4, "bob", "hello world");
		index.index(t1).actionGet();
		index.refresh();

		final Tweet t2 = new Tweet(2, 6, "alice", "hello bob");
		index.index(t2).actionGet();
		index.refresh();
		return index;
	}
}
