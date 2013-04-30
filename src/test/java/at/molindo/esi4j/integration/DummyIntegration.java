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
package at.molindo.esi4j.integration;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.common.settings.ImmutableSettings;

import at.molindo.esi4j.core.Esi4J;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.impl.DefaultEsi4J;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.rebuild.simple.SimpleRebuildProcessor;
import at.molindo.esi4j.test.module.Esi4JDummyModule;
import at.molindo.esi4j.test.util.Tweet;
import at.molindo.esi4j.test.util.TweetTypeMapping;

import com.google.common.collect.Lists;

public class DummyIntegration {

	public static void main(String[] args) {

		Esi4J esi4j = new DefaultEsi4J(ImmutableSettings.settingsBuilder().put("esi4j.client.type", "node")
				.put("index.store.type", "ram").put("index.refresh_interval", "-1").put("node.data", true)
				.put("node.local", true).put("gateway.type", "none").build());

		Esi4JIndex index = esi4j.getIndex();

		((InternalIndex) index).addTypeMapping(new TweetTypeMapping("tweet"));

		Tweet t1 = new Tweet(1, "sfussenegger", "esi4j rocks #elasticsearch");
		Tweet t4 = new Tweet(3, "msparer", "esi4j really rocks");

		Integer id = (Integer) index.index(t1).actionGet().getId();
		System.out.println("indexed: " + id);
		index.index(t4).actionGet().getId();

		index.refresh();

		Tweet t2 = (Tweet) index.get(Tweet.class, id).actionGet().getObject();
		System.out.println(t1.equals(t2));

		Tweet none = (Tweet) index.get(Tweet.class, 4711).actionGet().getObject();
		System.out.println(none == null);

		List<Tweet> tweets = index.multiGet(Tweet.class, Arrays.asList(new Integer[] { 1, 2, 3 })).actionGet()
				.getObjects(Tweet.class);
		System.out.println("multi: " + tweets.contains(t1));
		System.out.println("multi: " + tweets.contains(t4));

		t1.setUser("dummy");

		Esi4JDummyModule module = new Esi4JDummyModule().setData(Tweet.class, Lists.newArrayList(t1));
		new SimpleRebuildProcessor().rebuild(module, (InternalIndex) index, Tweet.class);

		Tweet t3 = (Tweet) index.get(Tweet.class, id).actionGet().getObject();
		System.out.println(t1.equals(t3));
	}
}
