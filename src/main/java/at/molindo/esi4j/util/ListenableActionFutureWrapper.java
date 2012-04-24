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
package at.molindo.esi4j.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.common.unit.TimeValue;

import at.molindo.utils.data.Function;

/**
 * a {@link ListenableActionFuture} that wraps another and transforms its result
 * using a {@link Function}
 */
public final class ListenableActionFutureWrapper<F, T> implements ListenableActionFuture<T> {

	private final ListenableActionFuture<F> _future;
	private final Function<F, T> _function;

	private F _raw;
	private T _result;

	public static <F, T> ListenableActionFutureWrapper<F, T> wrap(ListenableActionFuture<F> future,
			Function<F, T> function) {
		return new ListenableActionFutureWrapper<F, T>(future, function);
	}

	public ListenableActionFutureWrapper(ListenableActionFuture<F> future, Function<F, T> function) {
		if (future == null) {
			throw new NullPointerException("future");
		}
		if (function == null) {
			throw new NullPointerException("function");
		}
		_future = future;
		_function = function;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return _future.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return _future.isCancelled();
	}

	@Override
	public boolean isDone() {
		return _future.isDone();
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		return applyFunction(_future.get());
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return applyFunction(_future.get(timeout, unit));
	}

	@Override
	public T actionGet() throws ElasticSearchException {
		return applyFunction(_future.actionGet());
	}

	@Override
	public T actionGet(String timeout) throws ElasticSearchException {
		return applyFunction(_future.actionGet(timeout));
	}

	@Override
	public T actionGet(long timeoutMillis) throws ElasticSearchException {
		return applyFunction(_future.actionGet(timeoutMillis));
	}

	@Override
	public T actionGet(long timeout, TimeUnit unit) throws ElasticSearchException {
		return applyFunction(_future.actionGet(timeout, unit));
	}

	@Override
	public T actionGet(TimeValue timeout) throws ElasticSearchException {
		return applyFunction(_future.actionGet(timeout));
	}

	@Override
	public Throwable getRootFailure() {
		return _future.getRootFailure();
	}

	@Override
	public void addListener(final ActionListener<T> listener) {
		_future.addListener(new ActionListener<F>() {

			@Override
			public void onResponse(F response) {
				listener.onResponse(applyFunction(response));
			}

			@Override
			public void onFailure(Throwable e) {
				listener.onFailure(e);
			}
		});
	}

	@Override
	public void addListener(Runnable listener) {
		_future.addListener(listener);
	}

	private T applyFunction(F raw) {
		if (_result == null) {
			_raw = raw;
			_result = _function.apply(raw);
		} else if (_raw != raw) {
			throw new IllegalArgumentException("always same result expected");
		}
		return _result;
	}

}
