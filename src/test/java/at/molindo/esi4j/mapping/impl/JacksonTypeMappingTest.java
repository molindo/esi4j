package at.molindo.esi4j.mapping.impl;

import static org.junit.Assert.*;

import org.elasticsearch.action.index.IndexRequest;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.Data;
import lombok.experimental.Accessors;

public class JacksonTypeMappingTest {

	@Test
	public void test() throws Exception {
		final ObjectMapper m = new ObjectMapper();

		final JacksonTypeMapping mapping = new JacksonTypeMapping("foo", Foo.class, "id", "version", m);

		final Foo o = new Foo().setId(47).setVersion(11).setFoo("foo").setBar("bar");

		assertTrue(mapping.isVersioned());
		assertEquals(47, mapping.getId(o));
		assertEquals("47", mapping.toIdString(mapping.getId(o)));
		assertEquals(47, mapping.toId("47"));
		assertEquals(11, (long) mapping.getVersion(o));

		final IndexRequest r = new IndexRequest();
		mapping.getObjectSource(o).setSource(r);
		final String source = r.source().toUtf8();

		final ObjectNode obj = m.readValue(source, ObjectNode.class);
		assertEquals(47, obj.get("id").asInt());
	}

	@Data
	@Accessors(prefix = "", chain = true)
	public static class Foo {

		private Integer id;
		private Integer version;
		private String foo;
		private String bar;

	}

}
