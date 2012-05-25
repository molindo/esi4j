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

import java.util.Comparator;

import at.molindo.esi4j.mapping.TypeMapping;

import com.aconex.scrutineer.IdAndVersion;

public final class MappedIdAndVersionComparator implements Comparator<IdAndVersion> {
	private final TypeMapping _mapping;

	public MappedIdAndVersionComparator(TypeMapping mapping) {
		_mapping = mapping;
	}

	@Override
	@SuppressWarnings("unchecked")
	public int compare(IdAndVersion o1, IdAndVersion o2) {
		final Comparable<Object> i1 = o1 == null ? null : (Comparable<Object>) _mapping.toId(o1.getId());
		final Comparable<Object> i2 = o2 == null ? null : (Comparable<Object>) _mapping.toId(o2.getId());

		if (o1 == null) {
			return o2 == null ? 0 : 1;
		} else if (o2 == null) {
			return -1;
		} else {
			int compare = i1.compareTo(i2);
			if (compare == 0) {
				compare = o1.getVersion() < o2.getVersion() ? -1 : o1.getVersion() == o2.getVersion() ? 0 : 1;
			}
			return compare;
		}
	}
}