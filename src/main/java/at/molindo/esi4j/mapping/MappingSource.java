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

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * generator for elasticsearch mappings
 */
public interface MappingSource {

	/**
	 * sets source on request using appropriate setter
	 * 
	 * @param request
	 */
	void setSource(PutMappingRequestBuilder request);

	public final class Builder {

		private Builder() {
		}

		public static MappingSource builder(final XContentBuilder source) {
			return new MappingSource() {

				@Override
				public void setSource(PutMappingRequestBuilder request) {
					request.setSource(source);
				}
			};
		}

		public static MappingSource string(final String source) {
			return new MappingSource() {

				@Override
				public void setSource(PutMappingRequestBuilder request) {
					request.setSource(source);
				}
			};
		}

		public static MappingSource map(final Map<?, ?> source) {
			return new MappingSource() {

				@Override
				public void setSource(PutMappingRequestBuilder request) {
					request.setSource(source);
				}
			};
		}
	}
}
