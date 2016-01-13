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
package at.molindo.esi4j.action.impl;

import org.elasticsearch.action.count.CountResponse;

import at.molindo.esi4j.action.CountResponseWrapper;

public class DefaultCountResponseWrapper implements CountResponseWrapper {

	private final CountResponse _response;

	public DefaultCountResponseWrapper(final CountResponse response) {
		if (response == null) {
			throw new NullPointerException("response");
		}
		_response = response;
	}

	@Override
	public CountResponse getCountResponse() {
		return _response;
	}

}
