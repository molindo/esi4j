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
package at.molindo.esi4j.spring;

import java.util.Properties;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;

import at.molindo.esi4j.core.Esi4J;
import at.molindo.esi4j.core.impl.DefaultEsi4J;

/**
 * Esi4J spring bean. Override {@link #init(DefaultEsi4J)} to initialize
 */
public class Esi4JBean implements FactoryBean<Esi4J>, DisposableBean {

	private DefaultEsi4J _esi4j;
	private final Settings _defaultSettings;
	private Settings _settings;
	private Properties _properties;

	public Esi4JBean() {
		this(null);
	}

	public Esi4JBean(final Settings defaultSettings) {
		_defaultSettings = defaultSettings;
	}

	private DefaultEsi4J createEsi4J() {
		final Builder settings = ImmutableSettings.settingsBuilder();
		if (_defaultSettings != null) {
			settings.put(_defaultSettings);
		}
		if (_settings != null) {
			settings.put(_settings);
		}
		if (_properties != null) {
			settings.put(_properties);
		}

		_esi4j = newEsi4J(processSettings(settings).build());

		init(_esi4j);

		return _esi4j;
	}

	protected DefaultEsi4J newEsi4J(final Settings settings) {
		return new DefaultEsi4J(settings);
	}

	protected Builder processSettings(final Builder settings) {
		return settings;
	}

	protected void init(final DefaultEsi4J esi4j) {
	}

	@Override
	public final Esi4J getObject() throws Exception {
		if (_esi4j == null) {
			_esi4j = createEsi4J();
		}
		return _esi4j;
	}

	@Override
	public final Class<?> getObjectType() {
		return DefaultEsi4J.class;
	}

	@Override
	public final boolean isSingleton() {
		return true;
	}

	@Override
	public void destroy() throws Exception {
		if (_esi4j != null) {
			_esi4j.close();
		}
		close();
	}

	protected void close() {
	}

	public Properties getProperties() {
		return _properties;
	}

	public void setProperties(final Properties properties) {
		_properties = properties;
	}

	public Settings getSettings() {
		return _settings;
	}

	public void setSettings(final Settings settings) {
		_settings = settings;
	}

}
