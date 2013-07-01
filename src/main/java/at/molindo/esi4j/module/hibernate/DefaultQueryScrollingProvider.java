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
package at.molindo.esi4j.module.hibernate;

import java.util.Map;

import org.hibernate.FetchMode;

import com.google.common.collect.Maps;

public class DefaultQueryScrollingProvider extends AbstractHibernateScrollingProvider {

	private final Map<String, FetchMode> _fetchModes = Maps.newHashMap();

	public DefaultQueryScrollingProvider(Class<?> type) {
		super(type);
	}

	public DefaultQueryScrollingProvider setFetchMode(String associationPath, FetchMode fetchMode) {
		_fetchModes.put(associationPath, fetchMode);
		return this;
	}

	@Override
	public HibernateScrolling newScrolling() {
		return new DefaultQueryScrolling(getType(), _fetchModes);
	}

}
