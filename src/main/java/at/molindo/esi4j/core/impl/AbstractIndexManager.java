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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import at.molindo.esi4j.chain.Esi4JProcessingChain;
import at.molindo.esi4j.chain.impl.DefaultTaskSource;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.Esi4JIndexManager;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.module.Esi4JModule;
import at.molindo.esi4j.operation.RefreshOperation;
import at.molindo.esi4j.rebuild.Esi4JRebuildManager;
import at.molindo.esi4j.rebuild.impl.DefaultRebuildManager;
import at.molindo.utils.collections.ArrayUtils;

import com.google.common.collect.Sets;

public class AbstractIndexManager implements Esi4JIndexManager {

	private final InternalIndex _index;
	private final Esi4JModule _module;
	private final Class<?>[] _types;

	private final Esi4JProcessingChain _processingChain;
	private final Esi4JRebuildManager _rebuildManager;

	public AbstractIndexManager(Esi4JModule module, InternalIndex index, Esi4JProcessingChain processingChain) {
		this(module, index, processingChain, new DefaultRebuildManager());
	}

	public AbstractIndexManager(Esi4JModule module, InternalIndex index, Esi4JProcessingChain processingChain,
			Esi4JRebuildManager rebuildManager) {
		if (index == null) {
			throw new NullPointerException("index");
		}
		if (module == null) {
			throw new NullPointerException("module");
		}
		if (processingChain == null) {
			throw new NullPointerException("processingChain");
		}
		if (rebuildManager == null) {
			throw new NullPointerException("rebuildManager");
		}
		_index = index;
		_module = module;
		_processingChain = processingChain;
		_rebuildManager = rebuildManager;

		Set<Class<?>> types = Sets.newHashSet(_module.getTypes());
		types.retainAll(Arrays.asList(_index.getMappedTypes()));

		_types = types.toArray(new Class<?>[types.size()]);

		for (Class<?> type : _types) {
			if (!processingChain.getEventProcessor().isProcessing(type)) {
				processingChain.getEventProcessor().putTaskSource(type, new DefaultTaskSource());
			}
		}
	}

	@Override
	public Esi4JIndex getIndex() {
		return _index;
	}

	@Override
	public void rebuild(Class<?>... types) {
		if (ArrayUtils.empty(types)) {
			// by default use all supported types
			types = new Class<?>[_types.length];
			System.arraycopy(_types, 0, types, 0, _types.length);
		} else if (!Arrays.asList(_types).containsAll(Arrays.asList(types))) {
			HashSet<Class<?>> set = Sets.newHashSet(types);
			set.removeAll(Arrays.asList(_types));
			throw new IllegalArgumentException("can't rebuild unmanaged types: " + set);
		}

		_rebuildManager.rebuild(_module, _index, types);
	}

	@Override
	public void refresh() {
		_processingChain.getTaksProcessor().execute(new RefreshOperation());
	}

	@Override
	public final void close() {
		onBeforeClose();
		_module.close();
		_processingChain.getEventProcessor().close();
		_processingChain.getTaksProcessor().close();
		_rebuildManager.close();
		onAfterClose();
	}

	protected void onBeforeClose() {
	}

	protected void onAfterClose() {
	}

	@Override
	public Class<?>[] getTypes() {
		Class<?>[] types = new Class<?>[_types.length];
		System.arraycopy(_types, 0, types, 0, _types.length);
		return types;
	}

	protected Esi4JModule getModule() {
		return _module;
	}

	protected Esi4JProcessingChain getProcessingChain() {
		return _processingChain;
	}

}
