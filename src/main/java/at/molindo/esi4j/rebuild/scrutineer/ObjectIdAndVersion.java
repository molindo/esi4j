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

import com.aconex.scrutineer.IdAndVersion;

public class ObjectIdAndVersion extends IdAndVersion {

	private final Object _object;

	public ObjectIdAndVersion(Object object, String id, long version) {
		super(id, version);
		_object = object;
	}

	public Object getObject() {
		return _object;
	}

}