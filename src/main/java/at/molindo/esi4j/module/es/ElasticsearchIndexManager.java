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
package at.molindo.esi4j.module.es;

import org.elasticsearch.common.settings.Settings;

import at.molindo.esi4j.chain.Esi4JBatchedProcessingChain;
import at.molindo.esi4j.chain.Esi4JProcessingChain;
import at.molindo.esi4j.core.Esi4JIndex;
import at.molindo.esi4j.core.impl.AbstractIndexManager;
import at.molindo.esi4j.core.internal.InternalIndex;
import at.molindo.esi4j.rebuild.Esi4JRebuildManager;
import at.molindo.esi4j.rebuild.impl.DefaultRebuildManager;

public class ElasticsearchIndexManager extends AbstractIndexManager {

	public ElasticsearchIndexManager(final Settings transportClientSettings, final Esi4JIndex index, final Esi4JBatchedProcessingChain processingChain) {
		this(transportClientSettings, (InternalIndex) index, processingChain, new DefaultRebuildManager());
	}

	public ElasticsearchIndexManager(final Settings transportClientSettings, final InternalIndex index, final Esi4JProcessingChain processingChain, final Esi4JRebuildManager rebuildManager) {
		super(new ElasticsearchModule(transportClientSettings, index), index, processingChain, rebuildManager);
	}

}
