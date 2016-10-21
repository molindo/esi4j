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
package at.molindo.esi4j.mapping;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import com.google.common.collect.ImmutableMap;

public class ObjectReadSourceTest {

	@Test
	public void mapToString() throws Exception {
		final Map<String, Object> map = new LinkedHashMap<>();
		map.put("foo", 4711);
		map.put("bar", ImmutableMap.of("baz", "qux"));

		JSONAssert.assertEquals("{\"foo\":4711,\"bar\":{\"baz\":\"qux\"}}", ObjectReadSource.Builder.map("1", 1L, map)
				.string(), true);
	}
}
