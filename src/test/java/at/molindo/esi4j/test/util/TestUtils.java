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
package at.molindo.esi4j.test.util;

import java.io.File;
import java.util.UUID;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import at.molindo.esi4j.core.Esi4J;
import at.molindo.esi4j.core.Esi4JClient;
import at.molindo.esi4j.core.impl.DefaultEsi4J;
import at.molindo.esi4j.core.impl.NodeClient;
import at.molindo.utils.data.StringUtils;
import at.molindo.utils.io.FileUtils;
import at.molindo.utils.properties.SystemProperty;

public class TestUtils {

	private static final boolean HTTP = Boolean.getBoolean(TestUtils.class.getName() + ".http");

	private TestUtils() {
	}

	public static Esi4JClient newClient() {
		return newClient(null);
	}

	public static Esi4JClient newClient(String clusterName) {

		if (StringUtils.empty(clusterName)) {
			clusterName = ClusterName.DEFAULT.value();
		}

		final File tmp = newTmpDir();
		Settings settings = nodeSettings(tmp).put("cluster.name", clusterName).build();

		Node node = NodeBuilder.nodeBuilder().settings(settings).build();

		return new NodeClient(clusterName, node) {
			@Override
			public void close() {
				super.close();
				FileUtils.delete(tmp);
			}

		};
	}

	private static File newTmpDir() {
		final File tmp = new File(SystemProperty.JAVA_IO_TMPDIR.getFile(), "esi4jtest-" + UUID.randomUUID().toString());
		if (!tmp.mkdirs()) {
			throw new RuntimeException("failed to create temp dir: " + tmp);
		}
		return tmp;
	}

	private static Builder nodeSettings(File tmp) {
		return ImmutableSettings.settingsBuilder().put("path.data", new File(tmp, "data").toString())
				.put("path.logs", new File(tmp, "logs").toString()).put("index.store.type", "ram")
				.put("index.refresh_interval", "-1").put("node.data", true).put("node.local", true)
				.put("gateway.type", "none").put("http.enabled", HTTP).put("index.number_of_replicas", 0)
				.put("index.number_of_shards", 1);
	}

	public static Esi4J newEsi4j() {
		final File tmp = newTmpDir();

		return new DefaultEsi4J(nodeSettings(tmp).put("esi4j.client.type", "node").put("index.store.type", "ram")
				.put("index.refresh_interval", "-1").put("node.data", true).put("node.local", true)
				.put("gateway.type", "none").build()) {

			@Override
			public void close() {
				super.close();
				FileUtils.delete(tmp);
			}
		};
	}
}