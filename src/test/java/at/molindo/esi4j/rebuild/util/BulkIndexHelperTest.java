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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import at.molindo.esi4j.core.Esi4J;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.test.util.TestUtils;
import at.molindo.esi4j.test.util.Tweet;
import at.molindo.esi4j.test.util.TweetTypeMapping;

public class BulkIndexHelperTest {

	@Test
	public void test() throws Exception {
		Esi4J esi4j = TestUtils.newEsi4j();
		Esi4JIndex index = esi4j.getIndex();

		((InternalIndex) index).addTypeMapping(new TweetTypeMapping("tweet"));

		BulkIndexHelper helper = new BulkIndexHelper();
		BulkIndexHelper.IResponseHandler handler = createMock(BulkIndexHelper.IResponseHandler.class);
		handler.handle("1", "index");
		handler.handle("1", "delete");
		helper.setResponseHandler(handler);

		replay(handler);

		Tweet tweet = new Tweet("1", 4, "bob", "hello world");

		helper.newSession(index, 10).index(tweet).submit().await();
		assertEquals(1, helper.getSucceeded());
		assertEquals(0, helper.getFailed());
		index.refresh();

		assertEquals(tweet, index.get(Tweet.class, tweet.getId()).actionGet().getObject());

		helper.newSession(index, 10).delete(tweet).submit().await();
		assertEquals(2, helper.getSucceeded());
		assertEquals(0, helper.getFailed());
		index.refresh();

		assertNull(index.get(Tweet.class, tweet.getId()).actionGet().getObject());

		verify(handler);

		esi4j.close();
	}
}
