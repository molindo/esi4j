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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class ObjectIdAndVersionTest {

	@Test
	public void testNumberId() {
		final ObjectIdAndVersion i = new ObjectIdAndVersion(3, 0);
		assertEquals(3L, i.getRawId());
		assertEquals("3", i.getId());
		assertEquals(0L, i.getVersion());
		assertNull(i.getObject());
		assertEquals(new ObjectIdAndVersion(3, 0), i);
	}

	@Test
	public void testStringId() {
		final ObjectIdAndVersion i = new ObjectIdAndVersion("a", 0);
		assertEquals("a", i.getRawId());
		assertEquals("a", i.getId());
		assertEquals(0L, i.getVersion());
		assertNull(i.getObject());
		assertEquals(new ObjectIdAndVersion("a", 0), i);
	}

	@Test
	public void testCompare() {
		final List<ObjectIdAndVersion> list = Arrays
				.asList(new ObjectIdAndVersion(2, 0), new ObjectIdAndVersion(1, 0), new ObjectIdAndVersion(1, 1));
		Collections.sort(list);

		assertEquals(1L, list.get(0).getRawId());
		assertEquals(0L, list.get(0).getVersion());

		assertEquals(1L, list.get(1).getRawId());
		assertEquals(1L, list.get(1).getVersion());

		assertEquals(2L, list.get(2).getRawId());
	}
}
