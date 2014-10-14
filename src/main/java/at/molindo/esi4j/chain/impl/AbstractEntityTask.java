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
package at.molindo.esi4j.chain.impl;

import at.molindo.esi4j.chain.Esi4JEntityTask;

public abstract class AbstractEntityTask implements Esi4JEntityTask {

	private static final long serialVersionUID = 1L;

	private Object _entity;

	public AbstractEntityTask(Object entity) {
		setEntity(entity);
	}

	@Override
	public Esi4JEntityTask clone() {
		try {
			Esi4JEntityTask clone = (Esi4JEntityTask) super.clone();
			initClone(clone);
			return clone;
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException("clone not supported by Cloneable class", e);
		}
	}

	/**
	 * @return never null
	 */
	@Override
	public final Object getEntity() {
		return _entity;
	}

	protected final void setEntity(Object entity) {
		if (entity == null) {
			throw new NullPointerException("entity");
		}
		_entity = entity;
	}

	protected abstract void initClone(Esi4JEntityTask clone);
}
