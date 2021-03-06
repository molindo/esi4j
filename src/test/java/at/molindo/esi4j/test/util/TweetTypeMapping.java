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
package at.molindo.esi4j.test.util;

import static org.elasticsearch.index.mapper.MapperBuilders.*;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperBuilders;
import org.elasticsearch.index.mapper.internal.EnabledAttributeMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper.Dynamic;
import org.elasticsearch.index.mapper.object.RootObjectMapper.Builder;

import at.molindo.esi4j.mapping.impl.AbstractIntegerTypeMapping;

public class TweetTypeMapping extends AbstractIntegerTypeMapping<Tweet> {

	private static final String FIELD_MESSAGE = "message";
	private static final String FIELD_USER = "user";

	public TweetTypeMapping(final String typeAlias) {
		super(typeAlias, Tweet.class);
	}

	@Override
	protected Integer id(final Tweet o) {
		return o.getId();
	}

	@Override
	protected void setId(final Tweet o, final Integer id) {
		o.setId(id);
	}

	@Override
	public boolean isVersioned() {
		return true;
	}

	@Override
	protected Long version(final Tweet o) {
		return o.getVersion();
	}

	@Override
	protected void setVersion(final Tweet o, final Long version) {
		o.setVersion(version);
	}

	@Override
	protected void buildMapping(final Builder mapperBuilder) throws IOException {
		mapperBuilder.dynamic(Dynamic.STRICT).add(source().enabled(true))
				.add(all().enabled(EnabledAttributeMapper.ENABLED)).add(MapperBuilders.stringField(FIELD_MESSAGE))
				.add(MapperBuilders.stringField(FIELD_USER));
	}

	@Override
	protected void writeObject(final XContentBuilder contentBuilder, final Tweet o) throws IOException {
		contentBuilder.field(FIELD_MESSAGE, o.getMessage()).field(FIELD_USER, o.getUser());
	}

	@Override
	protected Tweet readObject(final Map<String, Object> source) {
		final Tweet tweet = new Tweet();
		tweet.setMessage((String) source.get(FIELD_MESSAGE));
		tweet.setUser((String) source.get(FIELD_USER));
		return tweet;
	}

}
