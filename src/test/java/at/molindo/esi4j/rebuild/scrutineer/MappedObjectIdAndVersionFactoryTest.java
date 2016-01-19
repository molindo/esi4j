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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

import at.molindo.esi4j.test.util.Tweet;
import at.molindo.esi4j.test.util.TweetTypeMapping;

public class MappedObjectIdAndVersionFactoryTest {

	private final TweetTypeMapping _mapping = new TweetTypeMapping("tweet");

	private final MappedObjectIdAndVersionFactory factory = new MappedObjectIdAndVersionFactory(_mapping);

	@Test
	public void serialize() throws IOException {
		final ObjectIdAndVersion id = new ObjectIdAndVersion(1L, 0, new Tweet(4711, "esi4j", "this should work"));

		final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		final ObjectOutputStream os = new ObjectOutputStream(bytes);

		factory.writeToStream(id, os);

		os.close();
		bytes.close();

		final ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
		final ObjectIdAndVersion read = factory.readFromStream(is);
		is.close();

		assertEquals(id.getId(), read.getId());
		assertEquals(id.getRawId(), read.getRawId());
		assertEquals(id.getVersion(), read.getVersion());
		assertEquals(id, read);
		assertEquals(0, id.compareTo(read));
	}
}
