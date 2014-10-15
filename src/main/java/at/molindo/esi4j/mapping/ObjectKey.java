package at.molindo.esi4j.mapping;

import java.io.Serializable;

/**
 * an identifier consisting of a type, a {@link Serializable} id and optionally
 * a version
 */
public final class ObjectKey implements Serializable {

	private static final long serialVersionUID = 1L;

	private final Class<?> _type;
	private final Serializable _id;
	private final Long _version;

	public ObjectKey(Class<?> type, Serializable id) {
		this(type, id, null);
	}

	public ObjectKey(Class<?> type, Serializable id, Long version) {
		if (type == null) {
			throw new NullPointerException("type");
		}
		if (id == null) {
			throw new NullPointerException("id");
		}
		_type = type;
		_id = id;
		_version = version;
	}

	/**
	 * @return the object type
	 */
	public Class<?> getType() {
		return _type;
	}

	/**
	 * @return never null
	 */
	public Serializable getId() {
		return _id;
	}

	/**
	 * @return the object version or null if not available
	 */
	public Long getVersion() {
		return _version;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (_id == null ? 0 : _id.hashCode());
		result = prime * result + (_type == null ? 0 : _type.getName().hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		ObjectKey other = (ObjectKey) obj;
		if (_id == null) {
			if (other._id != null) {
				return false;
			}
		} else if (!_id.equals(other._id)) {
			return false;
		}
		if (_type == null) {
			if (other._type != null) {
				return false;
			}
		} else if (!_type.equals(other._type)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder(_type.getSimpleName()).append("#").append(_id);
		if (_version != null) {
			buf.append(" (").append(_version).append(")");
		}
		return buf.toString();
	}

}