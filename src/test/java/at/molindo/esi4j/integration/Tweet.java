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
package at.molindo.esi4j.integration;

import org.elasticsearch.osem.annotations.Indexable;
import org.elasticsearch.osem.annotations.Searchable;

import com.google.common.base.Objects;

@Searchable
public final class Tweet {
	private String _id;
	private String _user;
	private String _message;

	public Tweet() {
	}

	public Tweet(String id, String user, String message) {
		setId(id);
		setUser(user);
		setMessage(message);
	}

	public String getId() {
		return _id;
	}

	@Indexable(indexName = "_id")
	public void setId(String id) {
		_id = id;
	}

	public String getUser() {
		return _user;
	}

	public void setUser(String user) {
		_user = user;
	}

	public String getMessage() {
		return _message;
	}

	public void setMessage(String message) {
		_message = message;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(_user, _message);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Tweet) {
			Tweet t = (Tweet) obj;
			return Objects.equal(_id, t._id) && Objects.equal(_user, t._user) && Objects.equal(_message, t._message);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(Tweet.class).add("id", getId()).add("user", getUser())
				.add("message", getMessage()).toString();
	}

}