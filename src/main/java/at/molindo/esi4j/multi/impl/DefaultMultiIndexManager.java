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
import java.util.LinkedHashSet;
import java.util.Set;

import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.multi.Esi4JManagedMultiIndex;
import at.molindo.esi4j.multi.Esi4JMultiIndexManager;
import at.molindo.utils.collections.ArrayUtils;
import at.molindo.utils.collections.IteratorUtils;
import at.molindo.utils.data.Function;

import com.google.common.collect.Sets;

/**
 * default implementation of {@link Esi4JMultiIndexManager}
 */
public class DefaultMultiIndexManager implements Esi4JMultiIndexManager {

	private final Esi4JManagedMultiIndex _index;
	private final Iterable<InternalIndex> _managedIndices;

	public DefaultMultiIndexManager(Esi4JManagedMultiIndex index) {
		if (index == null) {
			throw new NullPointerException("index");
		}
		_index = index;
		_managedIndices = IteratorUtils.readOnly(IteratorUtils.filter(getIndex().getIndices().values(),
				new Function<InternalIndex, Boolean>() {
					@Override
					public Boolean apply(InternalIndex index) {
						return index != null && index.getIndexManager() != null;
					}
				}));
	}

	/**
	 * rebuild supported types per wrapped index
	 */
	@Override
	public void rebuild(Class<?>... types) {
		if (ArrayUtils.empty(types)) {
			// default is to rebuild all
			for (InternalIndex index : getManagedIndices()) {
				index.getIndexManager().rebuild();
			}
		} else {
			for (InternalIndex index : getManagedIndices()) {

				// find type managed by this index manager
				LinkedHashSet<Class<?>> typeSet = Sets.newLinkedHashSet(Arrays.asList(types));
				typeSet.retainAll(Arrays.asList(index.getIndexManager().getTypes()));

				if (!typeSet.isEmpty()) {
					// only rebuild if any type managed by this manager
					Class<?>[] managedTypes = typeSet.toArray(new Class<?>[typeSet.size()]);
					index.getIndexManager().rebuild(managedTypes);
				}
			}
		}

	}

	@Override
	public void refresh() {
		for (InternalIndex index : getManagedIndices()) {
			index.getIndexManager().refresh();
		}
	}

	@Override
	public Class<?>[] getTypes() {
		Set<Class<?>> types = Sets.newLinkedHashSet();
		for (InternalIndex index : getManagedIndices()) {
			types.addAll(Arrays.asList(index.getIndexManager().getTypes()));
		}
		return types.toArray(new Class<?>[types.size()]);
	}

	@Override
	public final Esi4JManagedMultiIndex getIndex() {
		return _index;
	}

	protected final Iterable<InternalIndex> getManagedIndices() {
		return _managedIndices;
	}

	@Override
	public void close() {
	}
}