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
package at.molindo.esi4j.chain.impl;

import static org.easymock.EasyMock.createMock;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map.Entry;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

import at.molindo.esi4j.chain.Esi4JEntityTask;
import at.molindo.esi4j.chain.Esi4JTaskProcessor;
import at.molindo.esi4j.chain.Esi4JTaskSource;

public class DefaultEventProcessorTest {

	@Test
	public void processing() {
		DefaultEventProcessor processor = newProcessor();

		assertFalse(processor.isProcessing(Integer.class));

		processor.putTaskSource(Number.class, newSource());

		assertTrue(processor.isProcessing(Number.class));
		assertTrue(processor.isProcessing(Integer.class));
		assertFalse(processor.isProcessing(String.class));

		// nothing happens
		processor.removeTaskSource(Integer.class);
		assertTrue(processor.isProcessing(Number.class));
		assertTrue(processor.isProcessing(Integer.class));

		// remove
		processor.removeTaskSource(Number.class);
		assertFalse(processor.isProcessing(Number.class));
		assertFalse(processor.isProcessing(Integer.class));
	}

	@Test
	public void ignoredEvent() {
		DefaultEventProcessor processor = newProcessor();

		// no source, nothing happens

		replay(processor);

		processor.onPostInsert(4711);

		verify(processor);
	}

	@Test
	public void event() {

		DefaultEventProcessor processor = newProcessor();

		Esi4JTaskSource src = newSource();
		processor.putTaskSource(Number.class, src);

		Esi4JEntityTask[] tasks = new Esi4JEntityTask[] { newTask() };
		EasyMock.expect(src.getPostInsertTasks(4711)).andReturn(tasks);
		processor.getTaskProcessor().processTasks(tasks);

		replay(processor);

		assertTrue(processor.isProcessing(Integer.class));
		processor.onPostInsert(4711);

		verify(processor);
	}

	private void replay(DefaultEventProcessor processor) {
		EasyMock.replay(processor.getTaskProcessor());
		for (Entry<Class<?>, Esi4JTaskSource> e : processor.copyTaskSources().entrySet()) {
			EasyMock.replay(e.getValue());
		}
	}

	private void verify(DefaultEventProcessor processor) {
		EasyMock.verify(processor.getTaskProcessor());
		for (Entry<Class<?>, Esi4JTaskSource> e : processor.copyTaskSources().entrySet()) {
			EasyMock.verify(e.getValue());
		}
	}

	private DefaultEventProcessor newProcessor() {
		return new DefaultEventProcessor(createMock(Esi4JTaskProcessor.class));
	}

	private Esi4JTaskSource newSource() {
		return createMock(Esi4JTaskSource.class);
	}

	private Esi4JEntityTask newTask() {
		return EasyMock.createMock(Esi4JEntityTask.class);
	}

}
