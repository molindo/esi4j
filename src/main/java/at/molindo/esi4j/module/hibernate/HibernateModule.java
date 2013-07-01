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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.hibernate.EntityMode;
import org.hibernate.SessionFactory;
import org.hibernate.metadata.ClassMetadata;

import at.molindo.esi4j.module.Esi4JModule;
import at.molindo.esi4j.module.hibernate.scrolling.DefaultQueryScrollingSession;
import at.molindo.esi4j.module.hibernate.scrolling.ScrollingSession;
import at.molindo.esi4j.module.hibernate.scrolling.ScrollingSessionProvider;
import at.molindo.esi4j.rebuild.Esi4JRebuildSession;
import at.molindo.thirdparty.org.compass.core.util.concurrent.ConcurrentHashSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class HibernateModule implements Esi4JModule {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HibernateModule.class);

	private final SessionFactory _sessionFactory;

	private final ConcurrentHashSet<Class<?>> _rebuilding = new ConcurrentHashSet<Class<?>>();

	private final List<Class<?>> _types;

	private final ConcurrentMap<Class<?>, ScrollingSessionProvider> _scrollingProviders = Maps.newConcurrentMap();

	public HibernateModule(SessionFactory sessionFactory) {
		if (sessionFactory == null) {
			throw new NullPointerException("sessionFactory");
		}
		_sessionFactory = sessionFactory;

		// calculate types
		List<Class<?>> list = Lists.newArrayList();
		for (ClassMetadata classMetadata : sessionFactory.getAllClassMetadata().values()) {
			Class<?> type = classMetadata.getMappedClass(EntityMode.POJO);
			if (type != null) {
				list.add(type);
			}
		}
		_types = Collections.unmodifiableList(list);
	}

	@Override
	public Esi4JRebuildSession startRebuildSession(final Class<?> type) {
		if (type == null) {
			throw new NullPointerException("type");
		} else if (!_rebuilding.add(type)) {
			throw new IllegalStateException("already indexing " + type.getName());
		} else {
			return new HibernateRebuildSession(type, _sessionFactory, this, newScrollingSession(type));
		}
	}

	private ScrollingSession newScrollingSession(final Class<?> type) {
		ScrollingSessionProvider scrollingSessionProvider = _scrollingProviders.get(type);
		return scrollingSessionProvider == null ? new DefaultQueryScrollingSession(type) : scrollingSessionProvider.newScrollingSession();
	}

	void unsetRebuilding(Class<?> type) {
		if (!_rebuilding.remove(type)) {
			log.warn("type " + type.getName() + " not currently indexing");
		}
	}

	public void putScrollingProvider(ScrollingSessionProvider scrollingProvider) {
		if (scrollingProvider == null) {
			throw new NullPointerException("scrollingProvider");
		}
		// should we fail on duplicate types?
		_scrollingProviders.put(scrollingProvider.getType(), scrollingProvider);
	}

	@Override
	public Class<?>[] getTypes() {
		return _types.toArray(new Class<?>[_types.size()]);
	}

	@Override
	public void close() {
	}

	public SessionFactory getSessionFactory() {
		return _sessionFactory;
	}

}
