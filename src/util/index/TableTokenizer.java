package util.index;

import java.io.IOException;


import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import au.com.bytecode.opencsv.CSVReader;

public class TableTokenizer extends Tokenizer {
    private static final boolean SKIP_HEADERS = true;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

    private CSVReader csvReader;
    private String[] line = null;
    private int col = 0; //The column we are ready to read from.

    public TableTokenizer() {
        super();
    }

    @Override
    public boolean incrementToken() throws IOException {
//			if(lastReader != input)
//			{
//				updateReader();
//			}

        if (line == null) {
            if (SKIP_HEADERS) {
                line = csvReader.readNext();
            }
            line = csvReader.readNext();

            if (line == null) {
                return false;
            }
            termAtt.setEmpty();
            termAtt.append(line[0]);
            col = 1;
            return true;
        } else {
            if (col >= line.length) //exhausted line
            {
                line = csvReader.readNext();
                if (line == null) {
                    return false;
                }
                termAtt.setEmpty();
                termAtt.append(line[0]);
                col = 1;
                return true;
            } else {
                termAtt.setEmpty();
                termAtt.append(line[col]);
                col++;
                return true;
            }
        }
    }


    @Override
    public void reset() throws IOException {
        super.reset();
        posIncrAtt.setPositionIncrement(1);
        csvReader = new CSVReader(input);
        col = 0;
        line = null;

    }

}