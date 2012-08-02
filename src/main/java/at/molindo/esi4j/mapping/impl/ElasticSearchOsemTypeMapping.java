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
package at.molindo.esi4j.mapping.impl;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.osem.core.ObjectContext;
import org.elasticsearch.search.SearchHit;

import at.molindo.esi4j.mapping.MappingSource;
import at.molindo.esi4j.mapping.ObjectSource;
import at.molindo.esi4j.mapping.TypeMapping;

/**
 * {@link TypeMapping} using elasticsearch-osem's {@link ObjectContext}. One
 * context may be shared among multiple mappings.
 */
public class ElasticSearchOsemTypeMapping extends TypeMapping {

	private final ObjectContext _context;

	public static ElasticSearchOsemTypeMapping create(Class<?> typeClass, ObjectContext context) {
		String typeAlias = context.add(typeClass).getType(typeClass);
		return new ElasticSearchOsemTypeMapping(typeAlias, typeClass, context);
	}

	private ElasticSearchOsemTypeMapping(String typeAlias, Class<?> typeClass, ObjectContext context) {
		super(typeAlias, typeClass, String.class);
		if (context == null) {
			throw new NullPointerException("context");
		}
		_context = context;
	}

	@Override
	public String getId(Object o) {
		return _context.getId(o);
	}

	@Override
	public void setId(Object o, Object id) {
		// TODO only supports string ids
		_context.setId(o, toIdString(id));
	}

	@Override
	public String toIdString(Object id) {
		// TODO only supports string ids
		return (String) id;
	}

	@Override
	public Object toId(String id) {
		// TODO only supports string ids
		return id;
	}

	@Override
	public boolean isVersioned() {
		// TODO not supported
		return false;
	}

	@Override
	public Long getVersion(Object o) {
		// TODO not supported
		return null;
	}

	@Override
	public void setVersion(Object o, Long version) {
		// TODO not supported - noop
	}

	@Override
	public MappingSource getMappingSource() {
		return MappingSource.Builder.builder(_context.getMapping(getTypeClass()));
	}

	@Override
	public ObjectSource getObjectSource(Object o) {
		return ObjectSource.Builder.builder(_context.write(o));
	}

	@Override
	public Object read(GetResponse response) {
		return response.exists() ? _context.read(response) : null;
	}

	@Override
	public Object read(SearchHit hit) {
		return _context.read(hit);
	}

}
