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

import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.util.concurrent.ConcurrentMap;

import javax.transaction.Status;
import javax.transaction.Synchronization;

import org.hibernate.Transaction;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PostDeleteEvent;
import org.hibernate.event.spi.PostDeleteEventListener;
import org.hibernate.event.spi.PostInsertEvent;
import org.hibernate.event.spi.PostInsertEventListener;
import org.hibernate.event.spi.PostUpdateEvent;
import org.hibernate.event.spi.PostUpdateEventListener;

import com.google.common.collect.Maps;

import at.molindo.esi4j.chain.Esi4JBatchedEventProcessor;
import at.molindo.esi4j.chain.Esi4JBatchedEventProcessor.EventSession;

public class HibernateEventListener implements PostDeleteEventListener, PostInsertEventListener, PostUpdateEventListener {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HibernateEventListener.class);

	private static final long serialVersionUID = 1L;

	private transient final Esi4JBatchedEventProcessor _batchedEventProcessor;

	// TODO make weak for uncompleted transactions??
	private final ConcurrentMap<Transaction, EventSession> _map = Maps.newConcurrentMap();

	public HibernateEventListener(final Esi4JBatchedEventProcessor batchedEventProcessor) {
		if (batchedEventProcessor == null) {
			throw new NullPointerException("batchedEventProcessor");
		}
		_batchedEventProcessor = batchedEventProcessor;

	}

	@Override
	public void onPostInsert(final PostInsertEvent event) {
		final EventSession eventSession = findEventSession(event.getSession());
		if (eventSession != null) {
			eventSession.onPostInsert(event.getEntity());
		} else {
			_batchedEventProcessor.onPostInsert(event.getEntity());
		}
	}

	@Override
	public void onPostUpdate(final PostUpdateEvent event) {
		final EventSession eventSession = findEventSession(event.getSession());
		if (eventSession != null) {
			eventSession.onPostUpdate(event.getEntity());
		} else {
			_batchedEventProcessor.onPostUpdate(event.getEntity());
		}
	}

	@Override
	public void onPostDelete(final PostDeleteEvent event) {
		final EventSession eventSession = findEventSession(event.getSession());
		if (eventSession != null) {
			eventSession.onPostDelete(event.getEntity());
		} else {
			_batchedEventProcessor.onPostDelete(event.getEntity());
		}
	}

	private EventSession findEventSession(final EventSource hibernateSession) {
		if (hibernateSession.isTransactionInProgress()) {
			final Transaction transaction = hibernateSession.getTransaction();
			EventSession session = _map.get(transaction);
			if (session == null) {
				session = _batchedEventProcessor.startSession();

				transaction.registerSynchronization(new Esi4JHibernateSynchronization(transaction));

				_map.put(transaction, session);
			}
			return session;
		} else {
			return null;
		}
	}

	Object writeReplace() throws ObjectStreamException {
		// why are listeners serializable anyway?
		throw new NotSerializableException(HibernateEventListener.class.getName());
	}

	private final class Esi4JHibernateSynchronization implements Synchronization {

		private final Transaction _transaction;

		private Esi4JHibernateSynchronization(final Transaction transaction) {
			if (transaction == null) {
				throw new NullPointerException("transaction");
			}
			_transaction = transaction;
		}

		@Override
		public void beforeCompletion() {
		}

		@Override
		public void afterCompletion(final int status) {

			final EventSession session = _map.remove(_transaction);

			if (session == null) {
				log.error("no session registered for transaction");
			} else if (status == Status.STATUS_COMMITTED) {
				session.flush();
			}
		}
	}
}
