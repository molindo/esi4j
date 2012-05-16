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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import at.molindo.utils.collections.CollectionUtils;
import at.molindo.utils.data.StringUtils;

import com.google.common.collect.Maps;

/**
 * base class for mappings from type to elasticsearch representation
 */
public abstract class TypeMapping {

	public static final String FIELD_INDEX = "_index";
	public static final String FIELD_TYPE = "_type";
	public static final String FIELD_ID = "_id";
	public static final String FIELD_VERSION = "_version";

	private final String _typeAlias;
	private final Class<?> _typeClass;

	/**
	 * creates a {@link TypeMapping} for given alias and class. Both, alias and
	 * class must be unique per index.
	 */
	public TypeMapping(String typeAlias, Class<?> typeClass) {
		if (typeClass == null) {
			throw new NullPointerException("typeClass");
		}
		if (StringUtils.empty(typeAlias)) {
			throw new IllegalArgumentException("typeAlias must not be empty");
		}
		_typeAlias = typeAlias;
		_typeClass = typeClass;
	}

	/**
	 * @return the mapping's alias which translates directly to an elasticsearch
	 *         type
	 */
	public String getTypeAlias() {
		return _typeAlias;
	}

	/**
	 * TODO might this be a common base class?
	 */
	public Class<?> getTypeClass() {
		return _typeClass;
	}

	/**
	 * Simple filtering of entities. esi4j core library will always respect this
	 * filtering when working with mapped objects. Users are still free to
	 * persist entities anyway.
	 * 
	 * @return <code>true</code> if entity must not be persisted to index
	 */
	public boolean isFiltered(@Nonnull Object o) {
		return false;
	}

	/**
	 * @return the object's id
	 * @see #setId(Object, Object)
	 */
	public abstract Object getId(@Nonnull Object o);

	/**
	 * sets id on object
	 * 
	 * @see #getId(Object)
	 */
	public abstract void setId(Object o, Object id);

	/**
	 * @return the object's id as a string suitable for elasticsearch
	 * @see #getId(Object)
	 * @see #toIdString(Object)
	 */
	public final String getIdString(@Nonnull Object o) {
		Object id = getId(o);
		return id == null ? null : toIdString(id);
	}

	/**
	 * sets id on object after converting elasticsearch id to suitable type
	 * 
	 * @see #toId(String)
	 * @see #setId(Object, Object)
	 */
	public final Object setIdString(@Nonnull Object o, @Nonnull String idString) {
		Object id = toId(idString);
		setId(o, id);
		return id;
	}

	/**
	 * @return convert id to a string representation suitable for elasticsearch
	 * @see #toId(String)
	 */
	public abstract String toIdString(@Nonnull Object id);

	/**
	 * @return convert string representation suitable for elasticsearch to id
	 * @see #toIdString(Object)
	 */
	public abstract Object toId(@Nonnull String id);

	/**
	 * gets version from object or null if not versioned
	 */
	public abstract Long getVersion(Object o);

	/**
	 * set version on object
	 */
	public abstract void setVersion(Object o, Long version);

	/**
	 * @return a new {@link MappingSource} for this type
	 */
	public abstract MappingSource getMappingSource();

	/**
	 * @return a new {@link ObjectSource} for this object
	 */
	public abstract ObjectSource getObjectSource(Object o);

	/**
	 * @return the object returned by a {@link GetResponse}
	 */
	public abstract Object read(GetResponse response);

	/**
	 * @return the object returned by a {@link SearchHit}
	 */
	public abstract Object read(SearchHit hit);

	// some utilities

	/**
	 * @return map containing all properties of a {@link GetResponse}
	 * @see #getSource(SearchHit)
	 */
	protected Map<String, Object> getSource(GetResponse response) {
		Map<String, Object> map = response.getSource();
		if (map == null) {
			map = Maps.newHashMap();
			for (Entry<String, GetField> e : response.getFields().entrySet()) {
				List<?> values = e.getValue().getValues();
				if (!CollectionUtils.empty(values)) {
					map.put(e.getKey(), values.size() == 1 ? values.get(0) : values);
				}
			}
		}
		map.put(FIELD_INDEX, response.getIndex());
		map.put(FIELD_TYPE, response.getType());
		map.put(FIELD_ID, response.getId());
		map.put(FIELD_VERSION, response.getVersion());
		return map;
	}

	/**
	 * @return map containing all properties of a {@link SearchHit}
	 * @see #getSource(GetResponse)
	 */
	protected Map<String, Object> getSource(SearchHit hit) {
		Map<String, Object> map = hit.sourceAsMap();
		if (map == null) {
			map = Maps.newHashMap();
			for (Entry<String, SearchHitField> e : hit.getFields().entrySet()) {
				List<?> values = e.getValue().getValues();
				if (!CollectionUtils.empty(values)) {
					map.put(e.getKey(), values.size() == 1 ? values.get(0) : values);
				}
			}
		}
		map.put(FIELD_INDEX, hit.getIndex());
		map.put(FIELD_TYPE, hit.getType());
		map.put(FIELD_ID, hit.getId());
		map.put(FIELD_VERSION, hit.getVersion());
		return map;
	}

}
