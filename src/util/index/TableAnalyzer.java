package util.index;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.util.Version;

public class TableAnalyzer extends Analyzer {

    private Version version;


    public TableAnalyzer(Version luceneVersion) {
        this.version = luceneVersion;
    }


    @Override
    protected TokenStreamComponents createComponents(String fieldName) {

        final Tokenizer tableTokenizer = new TableTokenizer();
        TokenStream lowerCaseFilter = new LowerCaseFilter(tableTokenizer);

        return new TokenStreamComponents(tableTokenizer, lowerCaseFilter) {


            @Override
            protected void setReader(Reader reader) {
                super.setReader(reader);
                tableTokenizer.setReader(reader);

            }
        };
    }


}
