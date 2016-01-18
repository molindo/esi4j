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

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import com.google.common.base.Charsets;

/**
 * generator for elasticsearch object sources
 */
public interface ObjectWriteSource {

	void setSource(IndexRequestBuilder request);

	void setSource(IndexRequest request);

	/**
	 * get source as {@link Requests#INDEX_CONTENT_TYPE} ({@link XContentType#JSON JSON}) bytes
	 */
	BytesReference getSource();

	public final class Builder {

		private Builder() {
		}

		public static ObjectWriteSource builder(final XContentBuilder source) {
			return new ObjectWriteSource() {

				@Override
				public void setSource(final IndexRequestBuilder request) {
					request.setSource(source);
				}

				@Override
				public void setSource(final IndexRequest request) {
					request.source(source);
				}

				@Override
				public BytesReference getSource() {
					return source.bytes();
				}

			};
		}

		public static ObjectWriteSource string(final String source) {
			return new ObjectWriteSource() {

				@Override
				public void setSource(final IndexRequestBuilder request) {
					request.setSource(source);
				}

				@Override
				public void setSource(final IndexRequest request) {
					request.source(source);
				}

				@Override
				public BytesReference getSource() {
					return new BytesArray(source.getBytes(Charsets.UTF_8));
				}
			};
		}

		public static ObjectWriteSource map(final Map<String, Object> source) {
			return new ObjectWriteSource() {

				@Override
				public void setSource(final IndexRequestBuilder request) {
					request.setSource(source);
				}

				@Override
				public void setSource(final IndexRequest request) {
					request.source(source);
				}

				@Override
				public BytesReference getSource() {
					try (final XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
						builder.map(source);
						return builder.bytes();
					} catch (final IOException e) {
						throw new RuntimeException("building source failed", e);
					}
				}

			};
		}

		public static ObjectWriteSource map(final Map<String, Object> source, final XContentType contentType) {
			return new ObjectWriteSource() {

				@Override
				public void setSource(final IndexRequestBuilder request) {
					request.setSource(source, contentType);
				}

				@Override
				public void setSource(final IndexRequest request) {
					request.source(source, contentType);
				}

				@Override
				public BytesReference getSource() {
					try (final XContentBuilder builder = XContentFactory.contentBuilder(contentType)) {
						builder.map(source);
						return builder.bytes();
					} catch (final IOException e) {
						throw new RuntimeException("building source failed", e);
					}
				}

			};
		}
	}
}
