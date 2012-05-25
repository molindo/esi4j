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

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.object.RootObjectMapper.Builder;

import at.molindo.esi4j.mapping.impl.AbstractStringTypeMapping;

public class TweetTypeMapping extends AbstractStringTypeMapping<Tweet> {

	public TweetTypeMapping(String typeAlias) {
		super(typeAlias, Tweet.class);
	}

	@Override
	protected String id(Tweet o) {
		return o.getId();
	}

	@Override
	protected void id(Tweet o, String id) {
		o.setId(id);
	}

	@Override
	public boolean isVersioned() {
		return true;
	}

	@Override
	protected Long version(Tweet o) {
		return o.getVersion();
	}

	@Override
	protected void version(Tweet o, Long version) {
		o.setVersion(version);
	}

	@Override
	protected void buildMapping(Builder mapperBuilder) throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	protected void writeObject(XContentBuilder contentBuilder, Tweet o) throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	protected Tweet readObject(Map<String, Object> source) {
		// TODO Auto-generated method stub
		return null;
	}

}
