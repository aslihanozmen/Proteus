package webforms.wrappers;

import gnu.trove.map.TObjectIntMap;

import java.util.HashMap;
import java.util.Map;

public class HttpFormWrapper {
	private String wrapperType;
	private int outputStartingIndex;
	private int transformedExamples;
	private TObjectIntMap<String> outputpathOptions;
	private String url;
	public HttpFormWrapper(String type, String url, String inputParam, String outputpath,
			Map<String, String> selectIDs, int i, TObjectIntMap<String> outputOptions) {
		super();
		this.url = url;
		this.inputParam = inputParam;
		this.outputpath = outputpath;
		this.selectIDs = selectIDs;
		this.wrapperType = type;
		this.outputStartingIndex = i;
		this.transformedExamples = outputOptions.get(outputpath);
		this.outputpathOptions = outputOptions;
	}
	public String getWrapperType() {
		return wrapperType;
	}
	public void setWrapperType(String wrapperType) {
		this.wrapperType = wrapperType;
	}
	private String inputParam;
	private String outputpath;
	private Map<String, String> selectIDs = new HashMap<String, String>();
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getInputParam() {
		return inputParam;
	}
	public void setInputParam(String inputParam) {
		this.inputParam = inputParam;
	}
	public String getOutputpath() {
		return outputpath;
	}
	public void setOutputpath(String outputpath) {
		this.outputpath = outputpath;
	}
	public Map<String, String> getSelectIDs() {
		return selectIDs;
	}
	public void setSelectIDs(Map<String, String> selectIDs) {
		this.selectIDs = selectIDs;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(url);
		sb.append("\n input Parameter: ");
		sb.append(inputParam);
		sb.append("\n");
		sb.append("Additional Parameter");
		for (String id: selectIDs.keySet()) {
			sb.append("(");
			sb.append(selectIDs.get(id));
			sb.append(":");
			sb.append(id);
			sb.append(")");
		}
		sb.append("\n");
		sb.append("outputPath");
		sb.append(outputpath);
		return sb.toString();
	}
	public int getOutputStartingIndex() {
		return outputStartingIndex;
	}
	public void setOutputStartingIndex(int outputStartingIndex) {
		this.outputStartingIndex = outputStartingIndex;
	}
	public int getTransformedExamples() {
		return transformedExamples;
	}
	public void setTransformedExamples(int transformedExamples) {
		this.transformedExamples = transformedExamples;
	}
	public TObjectIntMap<String> getOutputpathOptions() {
		return outputpathOptions;
	}
	public void setOutputpathOptions(TObjectIntMap<String> outputpathOptions) {
		this.outputpathOptions = outputpathOptions;
	}
	public String getOutputPathOptionsString() {
		StringBuffer optionsBuffer = new StringBuffer();
		for (String option: outputpathOptions.keySet()) {
			optionsBuffer.append(option).append(" ").append(outputpathOptions.get(option)).append(" ");
		}
		return optionsBuffer.toString();
	}
	
	public String getSelectIDsString() {
		StringBuffer optionsBuffer = new StringBuffer();
		for (String id: selectIDs.keySet()) {
			optionsBuffer.append(id).append(" ").append(selectIDs.get(id)).append(" ");
		}
		return optionsBuffer.toString();
	}
}
