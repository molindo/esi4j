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
package at.molindo.esi4j.module.hibernate.scrolling;


public class CustomQueryScrollingSessionProvider extends AbstractScrollingSessionProvider {

	private final QueryProvider _provider;

	public CustomQueryScrollingSessionProvider(Class<?> type, QueryProvider provider) {
		super(type);
		if (provider == null) {
			throw new NullPointerException("provider");
		}
		_provider = provider;
	}

	@Override
	public ScrollingSession newScrollingSession() {
		return new CustomQueryScrollingSession(getType(), _provider);
	}

}
