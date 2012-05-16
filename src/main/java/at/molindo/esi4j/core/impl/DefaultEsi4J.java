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

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPerparer;

import at.molindo.esi4j.core.Esi4J;
import at.molindo.esi4j.core.Esi4JClient;
import at.molindo.esi4j.core.Esi4JClientFactory;
import at.molindo.esi4j.core.Esi4JFactory;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.Esi4JIndexManager;
import at.molindo.esi4j.core.Esi4JManagedIndex;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.multi.impl.DefaultManagedMultiIndex;
import at.molindo.esi4j.util.Esi4JUtils;
import at.molindo.utils.collections.CollectionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DefaultEsi4J implements Esi4J {

	private final Settings _settings;
	@SuppressWarnings("unused")
	private final Environment _environment;

	private final ConcurrentMap<String, Esi4JClient> _clients = Maps.newConcurrentMap();
	private final ConcurrentMap<String, InternalIndex> _indexes = Maps.newConcurrentMap();

	private final ConcurrentMap<String, Esi4JIndexManager> _indexManagers = Maps.newConcurrentMap();

	public DefaultEsi4J() {
		this(ImmutableSettings.settingsBuilder().build(), true);
	}

	public DefaultEsi4J(Settings settings) {
		this(settings, true);
	}

	public DefaultEsi4J(Settings settings, boolean loadConfigSettings) {
		Tuple<Settings, Environment> tuple = InternalSettingsPerparer.prepareSettings(settings, loadConfigSettings);
		_settings = settingsBuilder().put(tuple.v1()).put("esi4j.enabled", true).build();
		_environment = tuple.v2();

		configure();
	}

	private void configure() {
		configureClients();
		configureIndexes();
	}

	private void configureClients() {
		String[] clients = _settings.getAsArray("esi4j.clients", new String[] { Esi4J.DEFAULT_CLIENT });
		for (String clientName : clients) {
			Settings clientSettings = settingsBuilder().put(_settings)
					.put(Esi4JUtils.getSettings(_settings, "client." + clientName + ".", "esi4j.client.")).build();

			Class<? extends Esi4JClientFactory> factoryClass = clientSettings.getAsClass("esi4j.client.type",
					TransportClientFactory.class, "at.molindo.esi4j.core.impl.", "ClientFactory");

			Esi4JFactory<Esi4JClient> factory = Esi4JUtils.createObject(factoryClass, clientSettings);

			_clients.put(clientName, factory.create());
		}
	}

	private void configureIndexes() {
		String[] indexNames = _settings.getAsArray("esi4j.indexes", new String[] { Esi4J.DEFAULT_INDEX });
		for (String indexName : indexNames) {
			Settings indexSettings = settingsBuilder().put(_settings)
					.put(Esi4JUtils.getSettings(_settings, "esi4j.index." + indexName + ".", "esi4j.index.")).build();

			String clientName = indexSettings.get("esi4j.index.client", DEFAULT_CLIENT);
			Esi4JClient client = _clients.get(clientName);

			if (client == null) {
				// TODO better exception
				throw new NullPointerException("client");
			}

			_indexes.put(indexName, new DefaultIndex(indexName, indexSettings.getByPrefix("esi4j.index."),
					new DefaultStore(client, indexName)));
		}
	}

	@Override
	public InternalIndex getIndex() {
		return getIndex(DEFAULT_INDEX);
	}

	@Override
	public InternalIndex getIndex(String name) {
		InternalIndex index = _indexes.get(name);
		if (index == null) {
			throw new IllegalStateException("index '" + name + "' not configured");
		}
		return index;
	}

	@Override
	public Esi4JIndex findIndex(Class<?> type) {
		return (Esi4JIndex) findMultiIndex(type);
	}

	@Override
	public Esi4JManagedIndex getMultiIndex(String... names) {
		return getMultiIndex(Arrays.asList(names));
	}

	@Override
	public Esi4JManagedIndex getMultiIndex(List<String> names) {
		if (CollectionUtils.empty(names)) {
			return new DefaultManagedMultiIndex(_indexes.values());
		} else if (names.size() == 1) {
			String name = names.get(0);
			if ("*".equals(name)) {
				return new DefaultManagedMultiIndex(_indexes.values());
			} else {
				return getIndex(name);
			}
		} else {
			List<InternalIndex> indices = Lists.newArrayListWithCapacity(names.size());
			for (String name : names) {
				indices.add(getIndex(name));
			}
			return new DefaultManagedMultiIndex(indices);
		}
	}

	@Override
	public Esi4JManagedIndex findMultiIndex(Class<?>... types) {
		return findMultiIndex(Arrays.asList(types));
	}

	@Override
	public Esi4JManagedIndex findMultiIndex(List<Class<? extends Object>> types) {
		List<String> names = Lists.newArrayListWithCapacity(_indexes.size());

		for (Entry<String, InternalIndex> e : _indexes.entrySet()) {
			for (Class<?> type : types) {
				if (e.getValue().isMapped(type)) {
					names.add(e.getKey());
					break;
				}
			}
		}

		return getMultiIndex(names);
	}

	@Override
	public void close() {
		for (Esi4JClient client : _clients.values()) {
			client.close();
		}
	}

	@Override
	public void registerIndexManger(Esi4JIndexManager indexManager) {
		Esi4JManagedIndex index = indexManager.getIndex();
		String indexName = index.getName();

		if (_indexManagers.putIfAbsent(indexName, indexManager) == null) {
			index.setIndexManager(indexManager);
		} else {
			throw new IllegalStateException("already an index manager registered for index " + indexName);
		}
	}

}
