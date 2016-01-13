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
package at.molindo.esi4j.util;

import java.lang.reflect.Constructor;
import java.util.Map.Entry;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;

public class Esi4JUtils {

	/**
	 * tries to construct an object of given class by either invoking a constructor that takes {@link Settings} as sole
	 * argument or the default constructor.
	 */
	public static <T> T createObject(final Class<T> cls, @Nullable final Settings settings) {
		Constructor<T> constructor;
		try {
			constructor = cls.getConstructor(Settings.class);
			try {
				return constructor.newInstance(settings);
			} catch (final Exception e) {
				throw new ElasticsearchException("Failed to create instance [" + cls + "]", e);
			}
		} catch (final NoSuchMethodException e) {
			try {
				constructor = cls.getConstructor();
				try {
					return constructor.newInstance();
				} catch (final Exception e1) {
					throw new ElasticsearchException("Failed to create instance [" + cls + "]", e);
				}
			} catch (final NoSuchMethodException e1) {
				throw new ElasticsearchException("No constructor for [" + cls + "]");
			}
		}
	}

	/**
	 * tries to construct an object of given class by either invoking a constructor that takes object and
	 * {@link Settings} as arguments object only.
	 */
	public static <T> T createObject(final Class<T> cls, final Object arg, @Nullable final Settings settings) {
		Constructor<T> constructor;
		try {
			constructor = cls.getConstructor(arg.getClass(), Settings.class);
			try {
				return constructor.newInstance(arg, settings);
			} catch (final Exception e) {
				throw new ElasticsearchException("Failed to create instance [" + cls + "]", e);
			}
		} catch (final NoSuchMethodException e) {
			try {
				constructor = cls.getConstructor(arg.getClass());
				try {
					return constructor.newInstance(arg);
				} catch (final Exception e1) {
					throw new ElasticsearchException("Failed to create instance [" + cls + "]", e);
				}
			} catch (final NoSuchMethodException e1) {
				throw new ElasticsearchException("No constructor for [" + cls + "] and argument " + arg.getClass());
			}
		}
	}

	/**
	 * @return a new settings object containing all keys starting with oldPrefix whereas oldPrefix is replaced by
	 *         newPrefix
	 */
	public static Settings getSettings(final Settings settings, final String oldPrefix, final String newPrefix) {
		final Builder builder = ImmutableSettings.settingsBuilder();

		for (final Entry<String, String> e : settings.getByPrefix(oldPrefix).getAsMap().entrySet()) {
			builder.put(newPrefix + e.getKey(), e.getValue());
		}

		return builder.build();
	}

}
