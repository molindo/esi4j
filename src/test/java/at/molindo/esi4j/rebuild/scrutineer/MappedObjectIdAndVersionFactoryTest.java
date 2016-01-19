package at.molindo.esi4j.rebuild.scrutineer;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

import at.molindo.esi4j.test.util.Tweet;
import at.molindo.esi4j.test.util.TweetTypeMapping;

public class MappedObjectIdAndVersionFactoryTest {

	private final TweetTypeMapping _mapping = new TweetTypeMapping("tweet");

	private final MappedObjectIdAndVersionFactory factory = new MappedObjectIdAndVersionFactory(_mapping);

	@Test
	public void serialize() throws IOException {
		final ObjectIdAndVersion id = new ObjectIdAndVersion(1L, 0, new Tweet(4711, "esi4j", "this should work"));

		final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		final ObjectOutputStream os = new ObjectOutputStream(bytes);

		factory.writeToStream(id, os);

		os.close();
		bytes.close();

		final ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
		final ObjectIdAndVersion read = factory.readFromStream(is);
		is.close();

		assertEquals(id.getId(), read.getId());
		assertEquals(id.getRawId(), read.getRawId());
		assertEquals(id.getVersion(), read.getVersion());
		assertEquals(id, read);
		assertEquals(0, id.compareTo(read));
	}
}
