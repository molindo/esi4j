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
package at.molindo.esi4j.mapping;

import static at.molindo.esi4j.mapping.TypeMapping.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import com.google.common.collect.Maps;

import at.molindo.utils.collections.CollectionUtils;

/**
 * a turn a source into a map representation. supported sources are {@link SearchHit}, {@link GetResponse} or another
 * {@link Map}. Instances can be turned into objects using {@link TypeMapping#read(ObjectReadSource)}
 */
public interface ObjectReadSource {

	Map<String, Object> map();

	public final class Builder {

		private Builder() {
		}

		public static ObjectReadSource search(final SearchHit hit) {
			return new ObjectReadSource() {

				@Override
				public Map<String, Object> map() {
					Map<String, Object> map = hit.sourceAsMap();
					if (map == null) {
						map = Maps.newHashMap();
						for (final Entry<String, SearchHitField> e : hit.getFields().entrySet()) {
							final List<?> values = e.getValue().getValues();
							if (!CollectionUtils.empty(values)) {
								map.put(e.getKey(), values.size() == 1 ? values.get(0) : values);
							}
						}
					}
					map.put(FIELD_INDEX, hit.getIndex());
					map.put(FIELD_TYPE, hit.getType());
					map.put(FIELD_ID, hit.getId());
					if (hit.getVersion() != -1) {
						map.put(FIELD_VERSION, hit.getVersion());
					}
					return map;
				}

			};
		}

		public static ObjectReadSource get(final GetResponse response) {
			return new ObjectReadSource() {

				@Override
				public Map<String, Object> map() {
					if (!response.isExists()) {
						return null;
					}

					Map<String, Object> map = response.getSource();
					if (map == null) {
						map = Maps.newHashMap();
						for (final Entry<String, GetField> e : response.getFields().entrySet()) {
							final List<?> values = e.getValue().getValues();
							if (!CollectionUtils.empty(values)) {
								map.put(e.getKey(), values.size() == 1 ? values.get(0) : values);
							}
						}
					}
					map.put(FIELD_INDEX, response.getIndex());
					map.put(FIELD_TYPE, response.getType());
					map.put(FIELD_ID, response.getId());
					if (response.getVersion() != -1) {
						map.put(FIELD_VERSION, response.getVersion());
					}
					return map;
				}

			};
		}

		public static ObjectReadSource map(final Object id, final long version, final Map<String, Object> map) {
			return new ObjectReadSource() {

				@Override
				public Map<String, Object> map() {
					if (map == null) {
						return null;
					}
					final HashMap<String, Object> m = new HashMap<>(map);
					m.put(FIELD_ID, id);
					if (version != -1) {
						m.put(FIELD_VERSION, version);
					}
					return m;
				}

			};
		}
	}
}
