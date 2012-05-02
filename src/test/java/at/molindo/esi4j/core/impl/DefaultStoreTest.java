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
package at.molindo.esi4j.core.impl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

import org.elasticsearch.common.settings.Settings;
import org.testng.annotations.Test;

import at.molindo.esi4j.core.Esi4JClient;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.test.util.TestUtils;

public class DefaultStoreTest {

	@Test
	public void testNewSetting() {
		Settings from = settingsBuilder().build();
		Settings to = settingsBuilder().put("index.number_of_replicas", 0).build();
		testUpdateSettings(from, to);
	}

	@Test
	public void testObsoleteSetting() {
		Settings from = settingsBuilder().put("index.number_of_replicas", 0).build();
		Settings to = settingsBuilder().build();
		testUpdateSettings(from, to);
	}

	private void testUpdateSettings(Settings from, Settings to) {

		Esi4JClient client = TestUtils.newClient();

		DefaultStore store = new DefaultStore(client, DefaultStoreTest.class.getSimpleName().toLowerCase());

		InternalIndex i1 = createMock(InternalIndex.class);
		expect(i1.getSettings()).andReturn(from).anyTimes();

		InternalIndex i2 = createMock(InternalIndex.class);
		expect(i2.getSettings()).andReturn(to).anyTimes();

		replay(i1, i2);

		store.assertIndex(i1);
		store.assertIndex(i2);

		verify(i1, i2);

		client.close();
	}
}
