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

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import at.molindo.esi4j.rebuild.Esi4JRebuildSession;
import at.molindo.esi4j.rebuild.scrutineer.ModuleIdAndVersionStream.ModuleIdAndVersionStreamIterator;
import at.molindo.esi4j.test.module.Esi4JDummyModule;
import at.molindo.esi4j.test.util.Tweet;
import at.molindo.esi4j.test.util.TweetTypeMapping;

public class ModuleIdAndVersionStreamTest {

	@Test
	public void testIterator() {

		final TweetTypeMapping mapping = new TweetTypeMapping("tweet");

		final Tweet t1 = new Tweet(1, 1, "dummy", "Tweet #1");
		final Tweet t2 = new Tweet(2, 1, "dummy", "Tweet #2");
		final Tweet t3 = new Tweet(3, 1, "dummy", "Tweet #3");
		final Tweet t4 = new Tweet(4, 1, "dummy", "Tweet #4");
		final Tweet t5 = new Tweet(5, 1, "dummy", "Tweet #5");

		final int[] fetched = new int[1];
		final int[] processed = new int[1];

		final Esi4JDummyModule module = new Esi4JDummyModule() {

			@Override
			public Esi4JRebuildSession startRebuildSession(final Class<?> type) {
				final Esi4JRebuildSession session = super.startRebuildSession(type);

				return new Esi4JRebuildSession() {

					@Override
					public Class<?> getType() {
						return session.getType();
					}

					@Override
					public boolean isOrdered() {
						return session.isOrdered();
					}

					@Override
					public List<?> getNext(final int batchSize) {
						/*
						 * all must have been processed before fetching new as underlying session might clear previous
						 * state
						 */
						assertEquals(fetched[0], processed[0]);

						final List<?> list = session.getNext(batchSize);
						fetched[0] += list.size();

						return list;
					}

					private void assertEquals(final int i, final int j) {
						// TODO Auto-generated method stub

					}

					@Override
					public Object getMetadata() {
						return null;
					}

					@Override
					public void close() {
						assertEquals(fetched[0], processed[0]);
						session.close();
					}

				};
			}

		}.setData(Tweet.class, t1, t2, t3, t4, t5);

		final ModuleIdAndVersionStream stream = new ModuleIdAndVersionStream(module
				.startRebuildSession(Tweet.class), 2, mapping);
		stream.open();

		final ModuleIdAndVersionStreamIterator iter = (ModuleIdAndVersionStreamIterator) stream.iterator();

		while (iter.hasNext()) {
			final ObjectIdAndVersion next = (ObjectIdAndVersion) iter.next();
			assertNotNull(next);
			processed[0]++;
		}

		assertEquals(5, fetched[0]);
		assertEquals(5, processed[0]);

		stream.close();

	}
}
