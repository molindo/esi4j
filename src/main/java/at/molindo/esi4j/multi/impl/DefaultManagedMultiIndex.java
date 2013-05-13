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
package at.molindo.esi4j.multi.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.Esi4JIndexManager;
import at.molindo.esi4j.core.impl.AbstractIndex;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.mapping.TypeMapping;
import at.molindo.esi4j.multi.Esi4JManagedMultiIndex;
import at.molindo.esi4j.multi.Esi4JMultiStore;
import at.molindo.utils.data.StringUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DefaultManagedMultiIndex extends AbstractIndex implements Esi4JManagedMultiIndex {

	private final String _name;
	private final LinkedHashMap<String, InternalIndex> _indices = Maps.newLinkedHashMap();
	private Esi4JIndexManager _indexManager;
	private final Esi4JMultiStore _store;

	public DefaultManagedMultiIndex(Iterable<? extends InternalIndex> indices) {
		for (InternalIndex index : indices) {
			// keep insertion order
			_indices.put(index.getName(), index);
		}
		if (_indices.size() == 0) {
			throw new IllegalArgumentException("at least one index required");
		}
		_name = StringUtils.join(",", _indices.keySet());
		_store = new DefaultMultiStore(this);
	}

	@Override
	public String getName() {
		return _name;
	}

	@Override
	public boolean isMapped(Class<?> type) {
		for (InternalIndex index : _indices.values()) {
			if (index.isMapped(type)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isMapped(Object o) {
		for (InternalIndex index : _indices.values()) {
			if (index.isMapped(o)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String findIndexName(Class<?> type) {
		Esi4JIndex mappedIndex = null;

		for (InternalIndex index : _indices.values()) {
			if (index.isMapped(type)) {
				if (mappedIndex != null) {
					throw new IllegalArgumentException("can't find index for type mapped on multiple indices: "
							+ type.getName());
				} else {
					mappedIndex = index;
				}
			}
		}

		if (mappedIndex == null) {
			throw new IllegalArgumentException("unmapped type " + type.getName() + " for index " + getName());
		}

		return mappedIndex.getName();
	}

	@Override
	public TypeMapping findTypeMapping(Object o) {
		// return first match
		for (InternalIndex index : _indices.values()) {
			if (index.isMapped(o)) {
				return index.findTypeMapping(o);
			}
		}
		throw new IllegalArgumentException("type not mapped: " + o.getClass().getName());
	}

	@Override
	public TypeMapping findTypeMapping(Class<?> type) {
		// return first match
		for (InternalIndex index : _indices.values()) {
			if (index.isMapped(type)) {
				return index.findTypeMapping(type);
			}
		}
		throw new IllegalArgumentException("type not mapped: " + type.getName());
	}

	@Override
	public TypeMapping findTypeMapping(String indexName, String typeAlias) {
		for (InternalIndex index : _indices.values()) {
			if (index.getStore().getIndexName().equals(indexName)) {
				return index.findTypeMapping(indexName, typeAlias);
			}
		}
		throw new IllegalArgumentException("unexpected indexName: " + indexName);
	}

	@Override
	public Class<?>[] getMappedTypes() {
		Set<Class<?>> types = Sets.newLinkedHashSet();
		for (InternalIndex index : _indices.values()) {
			types.addAll(Arrays.asList(index.getMappedTypes()));
		}
		return types.toArray(new Class<?>[types.size()]);
	}

	@Override
	public Map<String, InternalIndex> getIndices() {
		return Collections.unmodifiableMap(_indices);
	}

	@Override
	protected Esi4JMultiStore getStore() {
		return _store;
	}

	@Override
	public void setIndexManager(Esi4JIndexManager indexManager) {
		throw new IllegalStateException("index contains index manager by default");
	}

	@Override
	public Esi4JIndexManager getIndexManager() {
		if (_indexManager == null) {
			_indexManager = new DefaultMultiIndexManager(this);
		}
		return _indexManager;
	}

}
