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

import static org.hibernate.event.spi.EventType.POST_COLLECTION_RECREATE;
import static org.hibernate.event.spi.EventType.POST_COLLECTION_REMOVE;
import static org.hibernate.event.spi.EventType.POST_COLLECTION_UPDATE;
import static org.hibernate.event.spi.EventType.POST_COMMIT_DELETE;
import static org.hibernate.event.spi.EventType.POST_COMMIT_INSERT;
import static org.hibernate.event.spi.EventType.POST_COMMIT_UPDATE;
import static org.hibernate.event.spi.EventType.POST_DELETE;
import static org.hibernate.event.spi.EventType.POST_INSERT;
import static org.hibernate.event.spi.EventType.POST_UPDATE;

import java.util.Iterator;

import org.hibernate.SessionFactory;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.PostCollectionRecreateEventListener;
import org.hibernate.event.spi.PostCollectionRemoveEventListener;
import org.hibernate.event.spi.PostCollectionUpdateEventListener;
import org.hibernate.event.spi.PostDeleteEventListener;
import org.hibernate.event.spi.PostInsertEventListener;
import org.hibernate.event.spi.PostUpdateEventListener;
import org.hibernate.internal.SessionFactoryImpl;

import at.molindo.esi4j.chain.Esi4JBatchedEventProcessor;

/**
 * Injects lifecycle listeners directly into Hibernate for mirroring operations.
 */
public class DefaultHibernateLifecycleInjector implements HibernateLifecycleInjector {

	private final boolean registerPostCommitListeneres;
	private Object _listener;

	public DefaultHibernateLifecycleInjector() {
		this(true);
	}

	/**
	 * Creates a new lifecycle injector. Allows to control if the
	 * insert/update/delete even listeners will be registered with post commit
	 * listeres (flag it <code>true</code>) or with plain post events (triggered
	 * based on Hibrenate flushing logic).
	 * 
	 * @param registerPostCommitListeneres
	 *            <code>true</code> if post commit listeners will be registered.
	 *            <code>false</code> for plain listeners.
	 */
	public DefaultHibernateLifecycleInjector(boolean registerPostCommitListeneres) {
		this.registerPostCommitListeneres = registerPostCommitListeneres;
	}

	@Override
	public synchronized void injectLifecycle(SessionFactory sessionFactory,
			Esi4JBatchedEventProcessor batchedEventProcessor) {
		if (_listener != null) {
			throw new IllegalStateException("already injected");
		}

		SessionFactoryImpl sessionFactoryImpl = (SessionFactoryImpl) sessionFactory;

		EventListenerRegistry registry = sessionFactoryImpl.getServiceRegistry()
				.getService(EventListenerRegistry.class);

		_listener = doCreateListener(sessionFactoryImpl, batchedEventProcessor);

		if (_listener instanceof PostInsertEventListener) {
			if (registerPostCommitListeneres) {
				registry.appendListeners(EventType.POST_COMMIT_INSERT, (PostInsertEventListener) _listener);
			} else {
				registry.appendListeners(EventType.POST_INSERT, (PostInsertEventListener) _listener);
			}
		}

		if (_listener instanceof PostUpdateEventListener) {
			if (registerPostCommitListeneres) {
				registry.appendListeners(EventType.POST_COMMIT_UPDATE, (PostUpdateEventListener) _listener);
			} else {
				registry.appendListeners(EventType.POST_UPDATE, (PostUpdateEventListener) _listener);
			}
		}

		if (_listener instanceof PostDeleteEventListener) {
			if (registerPostCommitListeneres) {
				registry.appendListeners(EventType.POST_COMMIT_DELETE, (PostDeleteEventListener) _listener);
			} else {
				registry.appendListeners(EventType.POST_DELETE, (PostDeleteEventListener) _listener);
			}
		}

		// collections
		if (!registerPostCommitListeneres) {
			if (_listener instanceof PostCollectionRecreateEventListener) {
				registry.appendListeners(EventType.POST_COLLECTION_RECREATE,
						(PostCollectionRecreateEventListener) _listener);
			}

			if (_listener instanceof PostCollectionRemoveEventListener) {
				registry.appendListeners(EventType.POST_COLLECTION_REMOVE,
						(PostCollectionRemoveEventListener) _listener);
			}

			if (_listener instanceof PostCollectionUpdateEventListener) {
				registry.appendListeners(EventType.POST_COLLECTION_UPDATE,
						(PostCollectionUpdateEventListener) _listener);
			}
		}
	}

	@Override
	public synchronized void removeLifecycle(SessionFactory sessionFactory) {
		if (_listener == null) {
			return;
		}

		SessionFactoryImpl sessionFactoryImpl = (SessionFactoryImpl) sessionFactory;

		EventListenerRegistry registry = sessionFactoryImpl.getServiceRegistry()
				.getService(EventListenerRegistry.class);

		if (registerPostCommitListeneres) {
			removeListeners(registry, _listener, POST_COMMIT_INSERT, POST_COMMIT_UPDATE, POST_COMMIT_DELETE);
		} else {
			removeListeners(registry, _listener, POST_INSERT, POST_UPDATE, POST_DELETE, POST_COLLECTION_RECREATE,
					POST_COLLECTION_UPDATE, POST_COLLECTION_REMOVE);
		}

		_listener = null;
	}

	private void removeListeners(EventListenerRegistry registry, Object listener, EventType<?>... eventTypes) {
		for (EventType<?> eventType : eventTypes) {
			Iterator<?> iter = registry.getEventListenerGroup(eventType).listeners().iterator();
			while (iter.hasNext()) {
				if (iter.next() == listener) {
					iter.remove();
				}
			}
		}
	}

	protected Object doCreateListener(SessionFactoryImpl sessionFactory,
			Esi4JBatchedEventProcessor batchedEventProcessor) {
		return new HibernateEventListener(batchedEventProcessor);
	}
}