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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.List;

import org.testng.annotations.Test;

import at.molindo.esi4j.rebuild.Esi4JRebuildSession;
import at.molindo.esi4j.rebuild.scrutineer.ModuleIdAndVersionStream.ModuleIdAndVersionStreamIterator;
import at.molindo.esi4j.test.module.Esi4JDummyModule;
import at.molindo.esi4j.test.util.Tweet;
import at.molindo.esi4j.test.util.TweetTypeMapping;

public class ModuleIdAndVersionStreamTest {

	@Test
	public void testIterator() {

		TweetTypeMapping mapping = new TweetTypeMapping("tweet");

		Tweet t1 = new Tweet(1, 1, "dummy", "Tweet #1");
		Tweet t2 = new Tweet(2, 1, "dummy", "Tweet #2");
		Tweet t3 = new Tweet(3, 1, "dummy", "Tweet #3");
		Tweet t4 = new Tweet(4, 1, "dummy", "Tweet #4");
		Tweet t5 = new Tweet(5, 1, "dummy", "Tweet #5");

		final int[] fetched = new int[1];
		final int[] processed = new int[1];

		Esi4JDummyModule module = new Esi4JDummyModule() {

			@Override
			public Esi4JRebuildSession startRebuildSession(Class<?> type) {
				final Esi4JRebuildSession session = super.startRebuildSession(type);

				return new Esi4JRebuildSession() {

					@Override
					public List<?> getNext(int batchSize) {
						/*
						 * all must have been processed before fetching new as
						 * underlying session might clear previous state
						 */
						assertEquals(fetched[0], processed[0]);

						List<?> list = session.getNext(batchSize);
						fetched[0] += list.size();

						return list;
					}

					@Override
					public void close() {
						assertEquals(fetched[0], processed[0]);
						session.close();
					}

				};
			}

		}.setData(Tweet.class, t1, t2, t3, t4, t5);

		ModuleIdAndVersionStream stream = new ModuleIdAndVersionStream(module, 2, mapping);
		stream.open();

		ModuleIdAndVersionStreamIterator iter = (ModuleIdAndVersionStreamIterator) stream.iterator();

		while (iter.hasNext()) {
			ObjectIdAndVersion next = (ObjectIdAndVersion) iter.next();
			assertNotNull(next);
			processed[0]++;
		}

		assertEquals(5, fetched[0]);
		assertEquals(5, processed[0]);

		stream.close();

	}
}
