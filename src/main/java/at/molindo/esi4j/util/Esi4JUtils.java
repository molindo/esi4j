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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;

public class Esi4JUtils {

	/**
	 * tries to construct an object of given class by either invoking a
	 * constructor that takes {@link Settings} as sole argument or the default
	 * constructor.
	 */
	public static <T> T createObject(Class<T> cls, @Nullable Settings settings) {
		Constructor<T> constructor;
		try {
			constructor = cls.getConstructor(Settings.class);
			try {
				return constructor.newInstance(settings);
			} catch (Exception e) {
				throw new ElasticSearchException("Failed to create instance [" + cls + "]", e);
			}
		} catch (NoSuchMethodException e) {
			try {
				constructor = cls.getConstructor();
				try {
					return constructor.newInstance();
				} catch (Exception e1) {
					throw new ElasticSearchException("Failed to create instance [" + cls + "]", e);
				}
			} catch (NoSuchMethodException e1) {
				throw new ElasticSearchException("No constructor for [" + cls + "]");
			}
		}
	}

	/**
	 * tries to construct an object of given class by either invoking a
	 * constructor that takes object and {@link Settings} as arguments object
	 * only.
	 */
	public static <T> T createObject(Class<T> cls, Object arg, @Nullable Settings settings) {
		Constructor<T> constructor;
		try {
			constructor = cls.getConstructor(arg.getClass(), Settings.class);
			try {
				return constructor.newInstance(arg, settings);
			} catch (Exception e) {
				throw new ElasticSearchException("Failed to create instance [" + cls + "]", e);
			}
		} catch (NoSuchMethodException e) {
			try {
				constructor = cls.getConstructor(arg.getClass());
				try {
					return constructor.newInstance(arg);
				} catch (Exception e1) {
					throw new ElasticSearchException("Failed to create instance [" + cls + "]", e);
				}
			} catch (NoSuchMethodException e1) {
				throw new ElasticSearchException("No constructor for [" + cls + "] and argument " + arg.getClass());
			}
		}
	}

	/**
	 * @return a new settings object containing all keys starting with oldPrefix
	 *         whereas oldPrefix is replaced by newPrefix
	 */
	public static Settings getSettings(Settings settings, String oldPrefix, String newPrefix) {
		Builder builder = ImmutableSettings.settingsBuilder();

		for (Entry<String, String> e : settings.getByPrefix(oldPrefix).getAsMap().entrySet()) {
			builder.put(newPrefix + e.getKey(), e.getValue());
		}

		return builder.build();
	}

}
