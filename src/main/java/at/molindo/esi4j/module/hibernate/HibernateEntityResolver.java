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
import java.util.IdentityHashMap;
import java.util.Map.Entry;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.internal.SessionImpl;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.proxy.HibernateProxy;

import at.molindo.esi4j.chain.Esi4JBatchedEntityResolver;
import at.molindo.esi4j.chain.Esi4JEntityTask;
import at.molindo.esi4j.chain.Esi4JSessionEntityResolver;
import at.molindo.esi4j.ex.EntityNotResolveableException;
import at.molindo.esi4j.mapping.ObjectKey;
import at.molindo.utils.collections.ArrayUtils;
import at.molindo.utils.collections.ClassMap;

public class HibernateEntityResolver implements Esi4JBatchedEntityResolver, Esi4JSessionEntityResolver {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HibernateEntityResolver.class);

	private final SessionFactory _sessionFactory;
	private final ClassMap<String> _entityNames = ClassMap.create();
	private final ThreadLocal<Session> _localSession = new ThreadLocal<>();

	/**
	 * this is an optimization while bulk resolving. Use
	 * {@link Session#load(Class, Serializable)} to resolve and fetch later
	 * using configured bulk fetching.
	 */
	private final ThreadLocal<EntityBatchResolve> _batchResolve = new ThreadLocal<>();

	public HibernateEntityResolver(SessionFactory sessionFactory) {
		if (sessionFactory == null) {
			throw new NullPointerException("sessionFactory");
		}

		for (Entry<String, ClassMetadata> e : sessionFactory.getAllClassMetadata().entrySet()) {

			Class<?> mappedClass = e.getValue().getMappedClass();

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

		Class<?> type = meta.getMappedClass();

		Serializable id = meta.getIdentifier(entity, (SessionImpl) session);
		Long version = toLongVersion(meta.getVersion(entity));

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

	@Override
	public void startResolveSession() {
		Session session = _localSession.get();

		if (session != null) {
			log.warn("session already open, now closing first");
			closeResolveSession();
			session = null;
		}

		session = getNewSession(getSessionFactory());
		session.setDefaultReadOnly(true);
		session.setCacheMode(CacheMode.GET);
		session.setFlushMode(FlushMode.MANUAL);
		session.beginTransaction();
		_localSession.set(session);
	}

	@Override
	public void closeResolveSession() {
		Session session = _localSession.get();
		if (session != null) {
			session.getTransaction().commit();
			session.clear();
			session.close();
			_localSession.set(null);
		} else {
			log.warn("session not open");
		}
	}

	@Override
	public Object resolveEntity(Object replacedEntity) {
		if (replacedEntity instanceof ObjectKey) {
			ObjectKey key = (ObjectKey) replacedEntity;

			Session session = _localSession.get();
			if (session == null) {
				throw new IllegalStateException("no session available");
			}

			EntityBatchResolve batchResolve = _batchResolve.get();

			// ignore version, use latest, use load for batch fetching
			Object resolvedEntity;
			if (batchResolve != null) {
				resolvedEntity = session.load(key.getType(), key.getId());
				batchResolve.resolved(resolvedEntity);
			} else {
				resolvedEntity = session.get(key.getType(), key.getId());
			}

			if (resolvedEntity == null) {
				log.error("can't resolve object " + key);
			}

			return resolvedEntity;
		} else {
			// not replaced
			return replacedEntity;
		}

	}

	@Override
	public void resolveEntities(Esi4JEntityTask[] tasks) {
		if (!ArrayUtils.empty(tasks)) {

			EntityBatchResolve batchResolve = new EntityBatchResolve(tasks.length);
			_batchResolve.set(batchResolve);
			try {
				for (int i = 0; i < tasks.length; i++) {
					Esi4JEntityTask task = tasks[i];
					if (task != null) {
						batchResolve.task(task);
						try {
							task.resolveEntity(this);
						} catch (EntityNotResolveableException e) {
							// this should never happen or something is wrong
							log.warn("can't resolve entity although proxy expected");
							tasks[i] = null;
						}
					}

				}
			} finally {
				_batchResolve.remove();
			}

			batchResolve.resolve(tasks);
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

	private final class EntityBatchResolve {

		private final IdentityHashMap<Esi4JEntityTask, Object> _resolved;
		private Esi4JEntityTask _task;

		private EntityBatchResolve(int expectedMaxSize) {
			_resolved = new IdentityHashMap<>(expectedMaxSize);
		}

		public void resolve(Esi4JEntityTask tasks[]) {
			for (int i = 0; i < tasks.length; i++) {
				Esi4JEntityTask task = tasks[i];
				if (task != null) {
					Object resolved = _resolved.get(task);
					if (resolved instanceof HibernateProxy) {
						try {
							((HibernateProxy) resolved).getHibernateLazyInitializer().initialize();
						} catch (ObjectNotFoundException e) {
							log.debug("can't initialize proxy, removing task");
							tasks[i] = null;
						}
					}
				}

			}
		}

		private void task(Esi4JEntityTask task) {
			_task = task;
		}

		private void resolved(Object resolvedEntity) {
			if (_task == null) {
				throw new IllegalStateException("no entity expected");
			}
			_resolved.put(_task, resolvedEntity);
			_task = null;
		}

	}
}
