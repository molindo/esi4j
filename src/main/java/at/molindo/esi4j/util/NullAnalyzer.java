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

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

/**
 * use this class for a {@link NamedAnalyzer} passed to
 * {@link AbstractFieldMapper.OpenBuilder#indexAnalyzer(NamedAnalyzer)} as it's really about the name only.
 */
public class NullAnalyzer extends Analyzer {

	public static final NullAnalyzer NULL_ANALYZER = new NullAnalyzer();

	@Override
	protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
		return new TokenStreamComponents(new KeywordTokenizer(reader));
	}

}
