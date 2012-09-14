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

import java.io.Serializable;
import java.util.Date;
import java.util.Map.Entry;

import org.hibernate.CacheMode;
import org.hibernate.EntityMode;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.impl.SessionImpl;
import org.hibernate.metadata.ClassMetadata;

import at.molindo.esi4j.chain.Esi4JEntityResolver;
import at.molindo.utils.collections.ClassMap;

public class HibernateEntityResolver implements Esi4JEntityResolver {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HibernateEntityResolver.class);

	private final SessionFactory _sessionFactory;
	private final ClassMap<String> _entityNames = ClassMap.create();
	private final ThreadLocal<Session> _localSession = new ThreadLocal<Session>();

	public HibernateEntityResolver(SessionFactory sessionFactory) {
		if (sessionFactory == null) {
			throw new NullPointerException("sessionFactory");
		}

		for (Entry<String, ClassMetadata> e : sessionFactory.getAllClassMetadata().entrySet()) {

			Class<?> mappedClass = e.getValue().getMappedClass(EntityMode.POJO);

			if (mappedClass != null) {
				_entityNames.put(mappedClass, e.getKey());
			}
		}

		_sessionFactory = sessionFactory;
	}

	/**
	 * must be called within originating session
	 */
	@Override
	public ObjectKey toObjectKey(Object entity) {
		SessionFactory factory = getSessionFactory();

		Session session = getCurrentSession(factory);

		String entityName = _entityNames.find(entity.getClass());

		ClassMetadata meta = factory.getClassMetadata(entityName);

		EntityMode entityMode = session.getEntityMode();
		Class<?> type = meta.getMappedClass(entityMode);

		Serializable id = meta.getIdentifier(entity, (SessionImpl) session);
		Long version = toLongVersion(meta.getVersion(entity, entityMode));

		return new ObjectKey(type, id, version);
	}

	private Long toLongVersion(Object version) {
		if (version instanceof Number) {
			return ((Number) version).longValue();
		} else if (version instanceof Date) {
			return ((Date) version).getTime();
		} else if (version != null) {
			log.warn("unexpected version type " + version.getClass().getName());
		}
		return null;
	}

	/**
	 * must be called within originating session
	 */
	@Override
	public Object replaceEntity(Object entity) {
		return toObjectKey(entity);
	}

	public void closeResolveSession() {
		Session session = _localSession.get();
		if (session != null) {
			session.getTransaction().commit();
			session.clear();
			session.close();
			_localSession.set(null);
		}
	}

	@Override
	public Object resolveEntity(Object replacedEntity) {
		if (replacedEntity instanceof ObjectKey) {
			ObjectKey key = (ObjectKey) replacedEntity;

			Session session = _localSession.get();
			if (session == null) {
				session = getNewSession(getSessionFactory());
				session.setDefaultReadOnly(true);
				session.setCacheMode(CacheMode.GET);
				session.setFlushMode(FlushMode.MANUAL);
				session.beginTransaction();
				_localSession.set(session);
			}

			// ignore version, use latest, use load for batch fetching
			Object resolvedEntity = session.load(key.getType(), key.getId());

			if (resolvedEntity == null) {
				log.error("can't resolve object " + key);
				return null;
			}

			return resolvedEntity;
		} else {
			// not replaced
			return replacedEntity;
		}

	}

	protected Session getCurrentSession(SessionFactory factory) {
		return factory.getCurrentSession();
	}

	protected Session getNewSession(SessionFactory factory) {
		return factory.openSession();
	}

	public final SessionFactory getSessionFactory() {
		return _sessionFactory;
	}

}
