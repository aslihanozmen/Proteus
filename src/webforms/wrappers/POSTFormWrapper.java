package webforms.wrappers;

import java.util.HashMap;
import java.util.Map;

public class POSTFormWrapper {
    private String url;

    public POSTFormWrapper(String url, String inputParam, String outputpath,
                           Map<String, String> selectIDs) {
        super();
        this.url = url;
        this.inputParam = inputParam;
        this.outputpath = outputpath;
        this.selectIDs = selectIDs;
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
        for (String id : selectIDs.keySet()) {
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
}

