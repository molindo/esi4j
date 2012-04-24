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

import java.util.List;

import org.hibernate.SessionFactory;
import org.hibernate.event.EventListeners;
import org.hibernate.event.PostCollectionRecreateEventListener;
import org.hibernate.event.PostCollectionRemoveEventListener;
import org.hibernate.event.PostCollectionUpdateEventListener;
import org.hibernate.event.PostDeleteEventListener;
import org.hibernate.event.PostInsertEventListener;
import org.hibernate.event.PostUpdateEventListener;
import org.hibernate.impl.SessionFactoryImpl;

import at.molindo.esi4j.chain.Esi4JBatchedEventProcessor;
import at.molindo.utils.collections.ArrayUtils;

import com.google.common.collect.Lists;

/**
 * Injects lifecycle listeners directly into Hibernate for mirroring operations.
 */
public class DefaultHibernateLifecycleInjector implements HibernateLifecycleInjector {

	private final boolean registerPostCommitListeneres;

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

	public void injectLifecycle(SessionFactory sessionFactory, Esi4JBatchedEventProcessor batchedEventProcessor) {

		SessionFactoryImpl sessionFactoryImpl = (SessionFactoryImpl) sessionFactory;
		EventListeners eventListeners = sessionFactoryImpl.getEventListeners();

		Object listener = doCreateListener(sessionFactoryImpl, batchedEventProcessor);

		if (listener instanceof PostInsertEventListener) {
			if (registerPostCommitListeneres) {
				PostInsertEventListener[] listeners = eventListeners.getPostCommitInsertEventListeners();
				listeners = ArrayUtils.append(listeners, (PostInsertEventListener) listener);
				eventListeners.setPostCommitInsertEventListeners(listeners);
			} else {
				PostInsertEventListener[] listeners = eventListeners.getPostInsertEventListeners();
				listeners = ArrayUtils.append(listeners, (PostInsertEventListener) listener);
				eventListeners.setPostInsertEventListeners(listeners);
			}
		}

		if (listener instanceof PostUpdateEventListener) {
			if (registerPostCommitListeneres) {
				PostUpdateEventListener[] listeners = eventListeners.getPostCommitUpdateEventListeners();
				listeners = ArrayUtils.append(listeners, (PostUpdateEventListener) listener);
				eventListeners.setPostCommitUpdateEventListeners(listeners);
			} else {
				PostUpdateEventListener[] listeners = eventListeners.getPostUpdateEventListeners();
				listeners = ArrayUtils.append(listeners, (PostUpdateEventListener) listener);
				eventListeners.setPostUpdateEventListeners(listeners);
			}
		}

		if (listener instanceof PostDeleteEventListener) {
			if (registerPostCommitListeneres) {
				PostDeleteEventListener[] listeners = eventListeners.getPostCommitDeleteEventListeners();
				listeners = ArrayUtils.append(listeners, (PostDeleteEventListener) listener);
				eventListeners.setPostCommitDeleteEventListeners(listeners);
			} else {
				PostDeleteEventListener[] listeners = eventListeners.getPostDeleteEventListeners();
				listeners = ArrayUtils.append(listeners, (PostDeleteEventListener) listener);
				eventListeners.setPostDeleteEventListeners(listeners);
			}
		}

		// collections
		if (registerPostCommitListeneres) {
			return;
		}

		if (listener instanceof PostCollectionRecreateEventListener) {
			PostCollectionRecreateEventListener[] listeners = eventListeners.getPostCollectionRecreateEventListeners();
			listeners = ArrayUtils.append(listeners, (PostCollectionRecreateEventListener) listener);
			eventListeners.setPostCollectionRecreateEventListeners(listeners);
		}

		if (listener instanceof PostCollectionRemoveEventListener) {
			PostCollectionRemoveEventListener[] listeners = eventListeners.getPostCollectionRemoveEventListeners();
			listeners = ArrayUtils.append(listeners, (PostCollectionRemoveEventListener) listener);
			eventListeners.setPostCollectionRemoveEventListeners(listeners);
		}

		if (listener instanceof PostCollectionUpdateEventListener) {
			PostCollectionUpdateEventListener[] listeners = eventListeners.getPostCollectionUpdateEventListeners();
			listeners = ArrayUtils.append(listeners, (PostCollectionUpdateEventListener) listener);
			eventListeners.setPostCollectionUpdateEventListeners(listeners);
		}
	}

	public void removeLifecycle(SessionFactory sessionFactory) {

		SessionFactoryImpl sessionFactoryImpl = (SessionFactoryImpl) sessionFactory;
		EventListeners eventListeners = sessionFactoryImpl.getEventListeners();

		PostInsertEventListener[] postInsertEventListeners;
		if (registerPostCommitListeneres) {
			postInsertEventListeners = eventListeners.getPostCommitInsertEventListeners();
		} else {
			postInsertEventListeners = eventListeners.getPostInsertEventListeners();
		}
		List<PostInsertEventListener> tempPostInsertEventListeners = Lists.newArrayList();
		for (int i = 0; i < postInsertEventListeners.length; i++) {
			PostInsertEventListener postInsertEventListener = postInsertEventListeners[i];
			if (!(postInsertEventListener instanceof HibernateEventListener)) {
				tempPostInsertEventListeners.add(postInsertEventListener);
			}
		}
		if (registerPostCommitListeneres) {
			eventListeners.setPostCommitInsertEventListeners((PostInsertEventListener[]) tempPostInsertEventListeners
					.toArray(new PostInsertEventListener[tempPostInsertEventListeners.size()]));
		} else {
			eventListeners.setPostInsertEventListeners((PostInsertEventListener[]) tempPostInsertEventListeners
					.toArray(new PostInsertEventListener[tempPostInsertEventListeners.size()]));
		}

		PostUpdateEventListener[] postUpdateEventListeners;
		if (registerPostCommitListeneres) {
			postUpdateEventListeners = eventListeners.getPostCommitUpdateEventListeners();
		} else {
			postUpdateEventListeners = eventListeners.getPostUpdateEventListeners();
		}
		List<PostUpdateEventListener> tempPostUpdateEventListeners = Lists.newArrayList();
		for (int i = 0; i < postUpdateEventListeners.length; i++) {
			PostUpdateEventListener postUpdateEventListener = postUpdateEventListeners[i];
			if (!(postUpdateEventListener instanceof HibernateEventListener)) {
				tempPostUpdateEventListeners.add(postUpdateEventListener);
			}
		}
		if (registerPostCommitListeneres) {
			eventListeners.setPostCommitUpdateEventListeners((PostUpdateEventListener[]) tempPostUpdateEventListeners
					.toArray(new PostUpdateEventListener[tempPostUpdateEventListeners.size()]));
		} else {
			eventListeners.setPostUpdateEventListeners((PostUpdateEventListener[]) tempPostUpdateEventListeners
					.toArray(new PostUpdateEventListener[tempPostUpdateEventListeners.size()]));
		}

		PostDeleteEventListener[] postDeleteEventListeners;
		if (registerPostCommitListeneres) {
			postDeleteEventListeners = eventListeners.getPostCommitDeleteEventListeners();
		} else {
			postDeleteEventListeners = eventListeners.getPostDeleteEventListeners();
		}
		List<PostDeleteEventListener> tempPostDeleteEventListeners = Lists.newArrayList();
		for (int i = 0; i < postDeleteEventListeners.length; i++) {
			PostDeleteEventListener postDeleteEventListener = postDeleteEventListeners[i];
			if (!(postDeleteEventListener instanceof HibernateEventListener)) {
				tempPostDeleteEventListeners.add(postDeleteEventListener);
			}
		}
		if (registerPostCommitListeneres) {
			eventListeners.setPostCommitDeleteEventListeners((PostDeleteEventListener[]) tempPostDeleteEventListeners
					.toArray(new PostDeleteEventListener[tempPostDeleteEventListeners.size()]));
		} else {
			eventListeners.setPostDeleteEventListeners((PostDeleteEventListener[]) tempPostDeleteEventListeners
					.toArray(new PostDeleteEventListener[tempPostDeleteEventListeners.size()]));
		}

		if (registerPostCommitListeneres) {
			return;
		}

		PostCollectionRecreateEventListener[] postCollectionRecreateEventListeners = eventListeners
				.getPostCollectionRecreateEventListeners();
		List<PostCollectionRecreateEventListener> tempPostCollectionRecreateEventListeners = Lists.newArrayList();
		for (PostCollectionRecreateEventListener postCollectionRecreateEventListener : postCollectionRecreateEventListeners) {
			if (!(postCollectionRecreateEventListener instanceof HibernateEventListener)) {
				tempPostCollectionRecreateEventListeners.add(postCollectionRecreateEventListener);
			}
		}
		eventListeners.setPostCollectionRecreateEventListeners(tempPostCollectionRecreateEventListeners
				.toArray(new PostCollectionRecreateEventListener[tempPostCollectionRecreateEventListeners.size()]));

		PostCollectionUpdateEventListener[] postCollectionUpdateEventListeners = eventListeners
				.getPostCollectionUpdateEventListeners();
		List<PostCollectionUpdateEventListener> tempPostCollectionUpdateEventListeners = Lists.newArrayList();
		for (PostCollectionUpdateEventListener postCollectionUpdateEventListener : postCollectionUpdateEventListeners) {
			if (!(postCollectionUpdateEventListener instanceof HibernateEventListener)) {
				tempPostCollectionUpdateEventListeners.add(postCollectionUpdateEventListener);
			}
		}
		eventListeners.setPostCollectionUpdateEventListeners(tempPostCollectionUpdateEventListeners
				.toArray(new PostCollectionUpdateEventListener[tempPostCollectionUpdateEventListeners.size()]));

		PostCollectionRemoveEventListener[] postCollectionRemoveEventListeners = eventListeners
				.getPostCollectionRemoveEventListeners();
		List<PostCollectionRemoveEventListener> tempPostCollectionRemoveEventListeners = Lists.newArrayList();
		for (PostCollectionRemoveEventListener postCollectionRemoveEventListener : postCollectionRemoveEventListeners) {
			if (!(postCollectionRemoveEventListener instanceof HibernateEventListener)) {
				tempPostCollectionRemoveEventListeners.add(postCollectionRemoveEventListener);
			}
		}
		eventListeners.setPostCollectionRemoveEventListeners(tempPostCollectionRemoveEventListeners
				.toArray(new PostCollectionRemoveEventListener[tempPostCollectionRemoveEventListeners.size()]));

	}

	protected Object doCreateListener(SessionFactoryImpl sessionFactory,
			Esi4JBatchedEventProcessor batchedEventProcessor) {
		return new HibernateEventListener(batchedEventProcessor);
	}
}