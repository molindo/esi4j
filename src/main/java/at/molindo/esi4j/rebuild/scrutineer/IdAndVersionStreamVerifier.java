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
package at.molindo.esi4j.rebuild.scrutineer;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.aconex.scrutineer.IdAndVersion;
import com.aconex.scrutineer.IdAndVersionStream;
import com.aconex.scrutineer.IdAndVersionStreamVerifierListener;
import com.aconex.scrutineer.LogUtils;

public class IdAndVersionStreamVerifier {

	private static final Logger LOG = LogUtils.loggerForThisClass();

	public void verify(final IdAndVersionStream primaryStream, final IdAndVersionStream secondayStream, final IdAndVersionStreamVerifierListener idAndVersionStreamVerifierListener) {
		long numItems = 0;
		final long begin = System.currentTimeMillis();

		try {

			parallelOpenStreamsAndWait(primaryStream, secondayStream);

			final Iterator<IdAndVersion> primaryIterator = primaryStream.iterator();
			final Iterator<IdAndVersion> secondaryIterator = secondayStream.iterator();

			IdAndVersion primaryItem = next(primaryIterator);
			IdAndVersion secondaryItem = next(secondaryIterator);

			while (primaryItem != null && secondaryItem != null) {
				if (primaryItem.equals(secondaryItem)) {
					primaryItem = verifiedNext(primaryIterator, primaryItem);
					secondaryItem = next(secondaryIterator);
				} else if (primaryItem.getId().equals(secondaryItem.getId())) {
					idAndVersionStreamVerifierListener.onVersionMisMatch(primaryItem, secondaryItem);
					primaryItem = verifiedNext(primaryIterator, primaryItem);
					secondaryItem = next(secondaryIterator);
				} else if (primaryItem.compareTo(secondaryItem) < 0) {
					idAndVersionStreamVerifierListener.onMissingInSecondaryStream(primaryItem);
					primaryItem = verifiedNext(primaryIterator, primaryItem);
				} else {
					idAndVersionStreamVerifierListener.onMissingInPrimaryStream(secondaryItem);
					secondaryItem = next(secondaryIterator);
				}
				numItems++;
			}

			while (primaryItem != null) {
				idAndVersionStreamVerifierListener.onMissingInSecondaryStream(primaryItem);
				primaryItem = verifiedNext(primaryIterator, primaryItem);
				numItems++;
			}

			while (secondaryItem != null) {
				idAndVersionStreamVerifierListener.onMissingInPrimaryStream(secondaryItem);
				secondaryItem = next(secondaryIterator);
				numItems++;
			}
		} finally {
			closeWithoutThrowingException(primaryStream);
			closeWithoutThrowingException(secondayStream);
		}
		LogUtils.infoTimeTaken(LOG, begin, numItems, "Completed verification");
	}

	// CHECKSTYLE:ON

	private void parallelOpenStreamsAndWait(final IdAndVersionStream primaryStream, final IdAndVersionStream secondaryStream) {
		try {
			final ExecutorService executorService = Executors.newFixedThreadPool(1);
			final Future<?> secondaryStreamFuture = executorService.submit(new OpenStreamRunner(secondaryStream));

			primaryStream.open();
			secondaryStreamFuture.get();

			executorService.shutdown();
		} catch (final Exception e) {
			throw new IllegalStateException("Failed to open one or both of the streams in parallel", e);
		}
	}

	private IdAndVersion verifiedNext(final Iterator<IdAndVersion> iterator, final IdAndVersion previous) {
		if (iterator.hasNext()) {
			final IdAndVersion next = iterator.next();
			if (next == null) {
				throw new IllegalStateException("primary stream must not return null");
			} else if (previous.compareTo(next) > 0) {
				throw new IllegalStateException("primary stream not ordered as expected: " + next + " followed "
						+ previous);
			} else {
				return next;
			}
		} else {
			return null;
		}
	}

	private IdAndVersion next(final Iterator<IdAndVersion> iterator) {
		if (iterator.hasNext()) {
			return iterator.next();
		} else {
			return null;
		}
	}

	private void closeWithoutThrowingException(final IdAndVersionStream idAndVersionStream) {
		try {
			idAndVersionStream.close();
		} catch (final Exception e) {
			LogUtils.warn(LOG, "Unable to close IdAndVersionStream", e);
		}
	}

	private static class OpenStreamRunner implements Runnable {
		private final IdAndVersionStream primaryStream;

		public OpenStreamRunner(final IdAndVersionStream primaryStream) {
			this.primaryStream = primaryStream;
		}

		@Override
		public void run() {
			primaryStream.open();
		}
	}
}
