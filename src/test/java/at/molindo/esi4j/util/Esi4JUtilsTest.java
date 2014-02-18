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
package at.molindo.esi4j.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

public class Esi4JUtilsTest {

	@Test
	public void getSettings() {
		Settings s1 = ImmutableSettings.settingsBuilder().put("foo.bar.baz", true).build();

		Settings s2 = Esi4JUtils.getSettings(s1, "foo.bar.", "foo.");
		assertTrue(s1.getAsBoolean("foo.bar.baz", false));
		assertFalse(s2.getAsBoolean("foo.bar.baz", false));
		assertFalse(s1.getAsBoolean("foo.baz", false));
		assertTrue(s2.getAsBoolean("foo.baz", false));
	}
}
