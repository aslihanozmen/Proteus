package webforms.index;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import webforms.wrappers.HttpFormWrapper;

public class IndexBuilder {
	public static void main(String [] args) {
		try {
			IndexBuilder ib = new IndexBuilder();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}

	private IndexWriter writer;

	public IndexBuilder() throws IOException {
		File indexPath = new File(Config.getProperty("indexDir"));
		Directory indexDir = FSDirectory.open(indexPath.toPath());
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig indexConfig = new IndexWriterConfig(analyzer);
//		indexConfig.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
		writer = new IndexWriter(indexDir, indexConfig);
	}

	public void indexItem(String url, String content, String Xcolumn, String Ycolumn, String xValues, String yValues, HttpFormWrapper wrapper) throws IOException {
		Document doc = new Document();

		doc.add(new StringField("url", url, Store.YES));
		doc.add(new TextField("inputColumn", Xcolumn, Store.YES));
		doc.add(new TextField("outputColumn", Ycolumn, Store.YES));
		doc.add(new TextField("content", content, Store.YES));
		doc.add(new TextField("Xs", xValues, Store.YES));
		doc.add(new TextField("Ys", yValues, Store.YES));
		doc.add(new StringField("requestUrl", wrapper.getUrl(), Store.YES));
		doc.add(new StringField("requestType", wrapper.getWrapperType(), Store.YES));
		doc.add(new StringField("inputParam", wrapper.getInputParam(), Store.YES));
		doc.add(new StringField("outputPath", wrapper.getOutputpath(), Store.YES));
		doc.add(new IntPoint("outputPathIndex"));
		doc.add(new TextField("pathOptions", wrapper.getOutputPathOptionsString(), Store.YES));
		doc.add(new TextField("selectBuffer", wrapper.getSelectIDsString(), Store.YES));
		

		writer.addDocument(doc);
		writer.commit();
		writer.close();
		

	}
	public void closeWriter() {
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

