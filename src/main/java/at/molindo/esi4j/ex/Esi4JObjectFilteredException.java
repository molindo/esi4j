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
package at.molindo.esi4j.ex;

import at.molindo.esi4j.mapping.TypeMapping;

public class Esi4JObjectFilteredException extends Esi4JRuntimeException {

	private static final long serialVersionUID = 1L;

	private final TypeMapping _mapping;
	private final Object _object;

	public Esi4JObjectFilteredException(TypeMapping mapping, Object object) {
		super("object filtered by mapping");

		if (mapping == null) {
			throw new NullPointerException("mapping");
		}
		if (object == null) {
			throw new NullPointerException("object");
		}
		_mapping = mapping;
		_object = object;
	}

	public TypeMapping getMapping() {
		return _mapping;
	}

	public Object getObject() {
		return _object;
	}

}
