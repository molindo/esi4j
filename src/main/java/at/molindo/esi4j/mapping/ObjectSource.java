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

import java.util.Map;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * generator for elasticsearch object sources
 */
public interface ObjectSource {
	void setSource(IndexRequestBuilder request);

	void setSource(IndexRequest request);

	public final class Builder {

		private Builder() {
		}

		public static ObjectSource builder(final XContentBuilder source) {
			return new ObjectSource() {

				@Override
				public void setSource(final IndexRequestBuilder request) {
					request.setSource(source);
				}

				@Override
				public void setSource(final IndexRequest request) {
					request.source(source);
				}
			};
		}

		public static ObjectSource string(final String source) {
			return new ObjectSource() {

				@Override
				public void setSource(final IndexRequestBuilder request) {
					request.setSource(source);
				}

				@Override
				public void setSource(final IndexRequest request) {
					request.source(source);
				}
			};
		}

		public static ObjectSource map(final Map<String, Object> source) {
			return new ObjectSource() {

				@Override
				public void setSource(final IndexRequestBuilder request) {
					request.setSource(source);
				}

				@Override
				public void setSource(final IndexRequest request) {
					request.source(source);
				}
			};
		}

		public static ObjectSource map(final Map<String, Object> source, final XContentType contentType) {
			return new ObjectSource() {

				@Override
				public void setSource(final IndexRequestBuilder request) {
					request.setSource(source, contentType);
				}

				@Override
				public void setSource(final IndexRequest request) {
					request.source(source);
				}
			};
		}
	}
}
