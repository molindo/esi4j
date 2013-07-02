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
package at.molindo.esi4j.test.module;

import java.util.Arrays;
import java.util.Collection;

import at.molindo.esi4j.module.Esi4JModule;
import at.molindo.esi4j.rebuild.Esi4JRebuildSession;
import at.molindo.esi4j.rebuild.util.IteratorRebuildSession;

public class Esi4JDummyModule implements Esi4JModule {

	private Class<?> _type;
	private Collection<? extends Object> _data;
	private boolean _ordered = false;

	public Esi4JDummyModule() {
	}

	@Override
	public Class<?>[] getTypes() {
		return new Class<?>[] { _type };
	}

	public <T> Esi4JDummyModule setData(Class<T> type, T... data) {
		return setData(type, Arrays.asList(data));
	}

	public <T> Esi4JDummyModule setData(Class<T> type, Collection<? extends T> data) {
		_type = type;
		_data = data;
		return this;
	}

	public boolean isOrdered() {
		return _ordered;
	}

	public Esi4JDummyModule setOrdered(boolean ordered) {
		_ordered = ordered;
		return this;
	}

	@Override
	public Esi4JRebuildSession startRebuildSession(Class<?> type) {
		if (_type != type) {
			throw new IllegalArgumentException("unexpected type " + type.getName());
		}
		return new IteratorRebuildSession(type, _data.iterator(), _ordered);
	}

	@Override
	public void close() {
	}

}
