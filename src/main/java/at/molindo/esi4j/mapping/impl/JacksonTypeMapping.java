package at.molindo.esi4j.mapping.impl;

import java.io.IOException;
import java.util.Iterator;

import org.elasticsearch.common.settings.Settings;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import at.molindo.esi4j.mapping.MappingSource;
import at.molindo.esi4j.mapping.ObjectReadSource;
import at.molindo.esi4j.mapping.ObjectWriteSource;
import at.molindo.esi4j.mapping.TypeMapping;

public class JacksonTypeMapping extends TypeMapping {

	private static String DEFAULT_ID_PROPERTY = "id";

	private final ObjectMapper _mapper;
	private final Class<?> _typeClass;

	private BeanPropertyWriter _idWriter;
	private BeanPropertyWriter _versionWriter;

	public JacksonTypeMapping(final String typeAlias, final Class<?> typeClass, final ObjectMapper mapper) {
		this(typeAlias, typeClass, DEFAULT_ID_PROPERTY, null, mapper);
	}

	public JacksonTypeMapping(final String typeAlias, final Class<?> typeClass, final String id, final ObjectMapper mapper) {
		this(typeAlias, typeClass, id, null, mapper);
	}

	public JacksonTypeMapping(final String typeAlias, final Class<?> typeClass, final String id, final String version, final ObjectMapper mapper) {
		super(typeAlias);

		if (typeClass == null) {
			throw new NullPointerException("typeClass");
		}
		if (id == null) {
			throw new NullPointerException("id");
		}
		if (mapper == null) {
			throw new NullPointerException("mapper");
		}

		_typeClass = typeClass;
		_mapper = mapper;

		try {
			final SerializerProvider serializerProvider = ((DefaultSerializerProvider) mapper.getSerializerProvider())
					.createInstance(mapper.getSerializationConfig(), mapper.getSerializerFactory());

			final JsonSerializer<Object> serializer = _mapper.getSerializerFactory()
					.createSerializer(serializerProvider, mapper.getSerializationConfig()
							.constructType(getTypeClass()));

			final Iterator<PropertyWriter> iter = serializer.properties();
			while (iter.hasNext()) {
				final PropertyWriter w = iter.next();
				if (id.equals(w.getName())) {
					_idWriter = (BeanPropertyWriter) w;
				} else if (version != null && version.equals(w.getName())) {
					_versionWriter = (BeanPropertyWriter) w;
					if (!Number.class.isAssignableFrom(_versionWriter.getPropertyType())) {
						throw new IllegalArgumentException("version property must be a Number, was "
								+ _versionWriter.getPropertyType());
					}
				}
			}

			if (_idWriter == null) {
				throw new IllegalArgumentException("id property '" + id + "' not found");
			}
			if (version != null && _versionWriter == null) {
				throw new IllegalArgumentException("version property '" + version + "' not found");
			}
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public Class<?> getTypeClass() {
		return _typeClass;
	}

	@Override
	public Class<?> getIdClass() {
		return _idWriter.getPropertyType();
	}

	@Override
	public boolean isVersioned() {
		return _versionWriter != null;
	}

	@Override
	public MappingSource getMappingSource(final Settings indexSettings) {
		return MappingSource.Builder.string("{}");
	}

	@Override
	public ObjectWriteSource getObjectSource(final Object o) {
		try {
			return ObjectWriteSource.Builder.string(_mapper.writeValueAsString(o));
		} catch (final JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object read(final ObjectReadSource hit) {
		try (final TokenBuffer buffer = new TokenBuffer(_mapper, false)) {
			_mapper.writeValue(buffer, hit.map());
			return _mapper.readValue(buffer.asParser(), getTypeClass());
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object getId(final Object o) {
		try {
			return _idWriter.get(o);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Long getVersion(final Object o) {
		try {
			final Number n = (Number) _versionWriter.get(o);
			return n == null ? null : n.longValue();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toIdString(final Object id) {
		try {
			return _mapper.writeValueAsString(id);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object toId(final String id) {
		try {
			return _mapper.readValue(id, _idWriter.getPropertyType());
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

}
