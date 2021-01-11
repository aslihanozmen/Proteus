package webforms.wrappers;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import util.WrapperUtilities;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.HttpMethod;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlOption;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.util.NameValuePair;

public class FormAnalyzer {

    public static void main(String[] args) {

        String inputColumn = "fraction";
        String outputColumn = "decimal";
        String url = "http://www.onlineconversion.com/fractions.php";
        String[] xValues = {"1/8", "23/56"};
        String[] yValues = {"0.125", "0.41"};

        FormAnalyzer fa = new FormAnalyzer(inputColumn, outputColumn, xValues,
                yValues);
        try {

            String formid = fa.getForms(url).keySet().toArray(new String[0])[1];
            ParameterFields fields =
                    fa.getSelectionFields(fa.getForms().get(""));
            System.out.println(fields);

            fa.getFormWrapperWith(formid, "fraction", new HashMap<String, String>(), url);
//			fa.analyze(url);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private HttpFormWrapper wrapper;

    private String inputColumn;

    private String outputColumn;

    private TObjectIntMap<String> outputFieldLength = new TObjectIntHashMap<String>();

    private TObjectIntMap<String> outputFieldWittnesses = new TObjectIntHashMap<String>();
    private TObjectIntMap<String> outputStartingIndex = new TObjectIntHashMap<String>();

    private String url;

    private final WebClient webClient = new WebClient(
            BrowserVersion.INTERNET_EXPLORER);

    private String[] xValues;

    public HashMap<String, String> getForms() {
        return forms;
    }

    public void setForms(HashMap<String, String> forms) {
        this.forms = forms;
    }

    private String[] yValues;

    private HashMap<String, String> forms;

    private String originalUrl;

    public FormAnalyzer(String inputColumn2, String outputColumn2,
                        String[] xValues2, String[] yValues2) {
        setInputColumn(inputColumn2);
        setOutputColumn(outputColumn2);
        setxValues(xValues2);
        setyValues(yValues2);
    }

    private String addParametersToURL(String url, String inputID,
                                      String xValue, Map<String, String> selectIDs) {
        StringBuilder sb = new StringBuilder(url);
        sb.append("?");
        sb.append(inputID);
        sb.append("=");
        sb.append(xValue);
        sb.append("&");
        for (String key : selectIDs.keySet()) {
            sb.append(selectIDs.get(key));
            sb.append("=");
            sb.append(key);
            sb.append("&");
        }
        return sb.toString();

    }

    public ParameterFields getSelectionFields(String formPath) {
        Map<String, Map<String, String>> selectionFields = new HashMap<String, Map<String, String>>();
        Map<String, String> fieldToCode = new HashMap<>();
        Set<String> inputFields = new HashSet<String>();
        Map<String, String> hiddenFields = new HashMap<String, String>();
        webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
        webClient.getOptions().setThrowExceptionOnScriptError(false);
        webClient.getOptions().setJavaScriptEnabled(false);

        HtmlPage page;
        try {
            page = (HtmlPage) webClient.getPage(url);
            // TODO
            // store selection fields and options
            List<HtmlElement> selectTags = page.getByXPath(formPath + "//select");
            boolean inputColumnOptionSet = false;
            for (HtmlElement tag : selectTags) {
                List<HtmlOption> optionTags = tag.getByXPath("option");
                boolean chooseDefaultTag = true;
                Map<String, String> textToAttributeName = new HashMap<String, String>();
                for (HtmlOption option : optionTags) {
                    textToAttributeName.put(option.getText(),
                            option.getValueAttribute());
                }
                selectionFields.put(tag.getAttribute("name"),
                        textToAttributeName);
                fieldToCode.put(tag.getAttribute("name"), tag.asXml());
            }

            List<HtmlElement> formElements = page.getByXPath(formPath + "//input");
            if (formElements.isEmpty()) {
                formElements = page.getByXPath("//input");
            }

            for (HtmlElement tag : formElements) {
                String tagType = tag.getAttribute("type");

                if (tagType.equals("radio")) {
                    String radioName = tag.getAttribute("name");
                    if (selectionFields.containsKey(radioName)) {

                        selectionFields.get(radioName).put(
                                tag.getAttribute("value"),
                                tag.getAttribute("value"));
                    } else {
                        Map<String, String> radioOption = new HashMap<String, String>();
                        radioOption.put(tag.getAttribute("value"),
                                tag.getAttribute("value"));
                        selectionFields.put(radioName, radioOption);
                    }
                }
                if (tagType.equals("text")
                        || tag.getAttribute("type").equals("") || tag.getAttribute("type").equals("search")) {
                    inputFields.add(tag.getAttribute("name"));// fill in
                    // new clas
                }
                if (tag.getAttribute("type").equals("hidden")) {
                    hiddenFields.put(tag.getAttribute("value"),
                            tag.getAttribute("name"));
                }
                fieldToCode.put(tag.getAttribute("name"), tag.asXml());

            }

        } catch (FailingHttpStatusCodeException | IOException e) {
            e.printStackTrace();
        }
        return new ParameterFields(selectionFields, inputFields, hiddenFields, fieldToCode);
    }

    public Map<String, String> getForms(String url) {
        this.url = url;
        Map<String, String> formToCode = new HashMap<>();
        forms = new HashMap<String, String>();
        webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
        webClient.getOptions().setThrowExceptionOnScriptError(false);
        webClient.getOptions().setJavaScriptEnabled(false);

        HtmlPage page;
        try {
            page = (HtmlPage) webClient.getPage(url);
            List<HtmlElement> formElements = page.getByXPath("/html/body//form");
            int formStringPosition = 0;
            for (HtmlElement formElement : formElements) {
                String formDescription = "";
                if (formElement.asText().length() > 0) {
                    String formText = formElement.asText();
                    if (formText.contains("\n")) {
                        formText = formText
                                .substring(0, formText.indexOf("\n"));
                    }
                    formDescription = formText.trim();
                    formToCode.put(formDescription, formElement.asXml());
                    forms.put(formDescription, formElement.getCanonicalXPath());
                } else {
                    String pageAsXml = page.asXml();
                    formStringPosition = pageAsXml.indexOf(formElement.asXml()
                                    .substring(0, formElement.asXml().indexOf(">")),
                            formStringPosition);
                    int headerPosition = 0;
                    int newheaderPosition = 0;
                    while (newheaderPosition < formStringPosition) {
                        headerPosition = newheaderPosition;
                        newheaderPosition = pageAsXml.indexOf("<h1>",
                                headerPosition + 4);
                        if (newheaderPosition == -1
                                || headerPosition == newheaderPosition) {
                            break;// no headers for this form
                        }
                    }
                    if (headerPosition > 0) {
                        formDescription = pageAsXml.substring(
                                headerPosition + 4,
                                pageAsXml.indexOf("</h1>", headerPosition));
                        formToCode.put(formDescription, formElement.asXml());
                        forms.put(formDescription,
                                formElement.getCanonicalXPath());
                    } else {
                        formToCode.put("Form without label: "
                                + formElement.getCanonicalXPath(), formElement.asXml());
                        forms.put(
                                "Form without label: "
                                        + formElement.getCanonicalXPath(),
                                formElement.getCanonicalXPath());
                    }

                }
            }
        } catch (FailingHttpStatusCodeException | IOException e) {
            e.printStackTrace();
        }

        return formToCode;
    }

    public boolean analyze(String urlString) throws Exception {
        wrapper = null;
        webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
        webClient.getOptions().setThrowExceptionOnScriptError(false);
        webClient.getOptions().setJavaScriptEnabled(false);
        // webClient.getOptions().setJavaScriptEnabled(true);
        url = originalUrl = urlString;
        try {
            final HtmlPage page = (HtmlPage) webClient.getPage(urlString);
            List<HtmlElement> forms = page.getByXPath("/html/body//form");

            for (HtmlElement form : forms) {
                // forms.
                outputFieldWittnesses.clear();
                outputStartingIndex.clear();
                outputFieldLength.clear();
                String method = form.getAttribute("method");
                System.out.println(form.asXml());
                url = createRequestURL(form);
                if (method.isEmpty() || method.toLowerCase().equals("get")) {
                    if (findRelevantGetFields(page, form)) {
                        // transformValuesGET(xValues);
                        return true;
                    }
                } else if (method.isEmpty()) {
                    if (form.asXml().contains("javascript")) {
                        // TODO go for JAVA script
                    } else {
                        // assume default GET
                        if (findRelevantGetFields(page, form)) {
                            return true;
                            // break;
                        }
                    }
                } else if (method.toLowerCase().equals("post")) {
                    if (findRelevantPostFields(page, form)) {
                        System.out.println("Say yeah");
                        return true;
                        // break;
                    }
                }
            }
            if (forms.isEmpty() || outputFieldWittnesses.isEmpty()) {
                webClient.getOptions().setJavaScriptEnabled(true);
                findRelevantFieldsJS(page);
            }
        } catch (ClassCastException e) {
            e.printStackTrace();
        }
        return false;

    }

    public HttpFormWrapper getGetWrapper() {
        return wrapper;
    }

    public void setGetWrapper(HttpFormWrapper getWrapper) {
        this.wrapper = getWrapper;
    }

    private List<ArrayList<NameValuePair>> combineRadioAndSubmitFields(
            Map<String, String> submitIDs, Map<String, String> radioIDs) {
        List<ArrayList<NameValuePair>> submitOptions = new ArrayList<ArrayList<NameValuePair>>();
        submitOptions.add(new ArrayList<NameValuePair>());
        for (String submitID : submitIDs.keySet()) {
            NameValuePair newSubmitPair = new NameValuePair(
                    submitIDs.get(submitID), submitID);
            // if (radioIDs.isEmpty()) {
            ArrayList<NameValuePair> newList = new ArrayList<NameValuePair>();
            newList.add(newSubmitPair);
            submitOptions.add(newList);
        }
        // }
        return submitOptions;
    }

    private String createRequestURL(HtmlElement form) {
        String newPath = form.getAttribute("action");
        if (newPath.equals("")) {
            return url;
        }
        if (!newPath.contains("http")) {
            String newURL = this.url;
            int slashAt = this.url.indexOf("/");
            slashAt = this.url.indexOf("/", slashAt + 1);
            slashAt = this.url.indexOf("/", slashAt + 1);

            if (slashAt == -1) {
                if (!newPath.startsWith("/")) {
                    newURL = newURL + "/";
                }
                newURL = newURL + newPath;
            } else {

                if (newPath.startsWith("/")) {
                    newURL = this.url.substring(0, slashAt) + newPath;
                } else {
                    int newSlashAt = slashAt;
                    while (newSlashAt != -1) {
                        slashAt = newSlashAt;
                        newSlashAt = this.url.indexOf("/", slashAt + 1);
                    }
                    newURL = this.url.substring(0, slashAt + 1) + newPath;
                }
            }
            return newURL;
        } else {
            return newPath;
        }

    }

    private WebRequest createWebRequest(String newURL, String inputID,
                                        String xValue, Map<String, String> selectIDs)
            throws MalformedURLException {
        WebRequest requestSettings = new WebRequest(new URL(newURL),
                HttpMethod.POST);

        // Set the request parameters
        requestSettings.setRequestParameters(new ArrayList());

        requestSettings.getRequestParameters().add(
                new NameValuePair(inputID, xValue));
        // requestSettings.getRequestParameters().add(
        // new NameValuePair("translations_count", "0"));
        for (String key : selectIDs.keySet()) {
            requestSettings.getRequestParameters().add(
                    new NameValuePair(selectIDs.get(key), key));
        }
        return requestSettings;
    }

    private Map<String, String> extractSelectionIDs(HtmlElement form) {
        Map<String, String> selectIDs = new HashMap<String, String>();
        List<HtmlElement> selectTags = form.getByXPath("//select");
        boolean inputColumnOptionSet = false;
        for (HtmlElement tag : selectTags) {
            List<HtmlOption> optionTags = tag.getByXPath("option");
            boolean chooseDefaultTag = true;
            for (HtmlOption option : optionTags) {
                if (option.getText().contains(inputColumn)
                        || option.toString().contains(inputColumn)) {
                    if (!inputColumnOptionSet) {
                        selectIDs.put(option.getValueAttribute(),
                                tag.getAttribute("name"));
                        chooseDefaultTag = false;
                        inputColumnOptionSet = true;
                        break;
                    }
                }
                if ((option.getText().contains(outputColumn) || option
                        .toString().contains(outputColumn))
                        && !(option.getText().contains(inputColumn))) {
                    selectIDs.put(option.getValueAttribute(),
                            tag.getAttribute("name"));
                    chooseDefaultTag = false;
                    break;
                }
            }
        }
        return selectIDs;
    }

    private void fillOutputFields(List<HtmlElement> outputTags,
                                  String exampleOutput) {
        for (HtmlElement outputTag : outputTags) {
            String path = outputTag.getCanonicalXPath();
            if (outputFieldWittnesses.containsKey(path)) {
                outputFieldWittnesses.put(path,
                        outputFieldWittnesses.get(path) + 1);
                outputFieldLength.put(path, outputFieldLength.get(path)
                        + outputTag.getTextContent().length());
            } else {
                outputFieldWittnesses.put(path, 1);
                outputStartingIndex.put(path, outputTag.getTextContent()
                        .toLowerCase().indexOf(exampleOutput.toLowerCase()));

                outputFieldLength
                        .put(path, outputTag.getTextContent().length());
            }
            System.out.println("result witnessed "
                    + outputFieldWittnesses.get(path) + " times in " + path);
        }
    }

    private String findOutputFieldGET(String inputID,
                                      Map<String, String> selectIDs)
            throws FailingHttpStatusCodeException, MalformedURLException,
            IOException {
        for (int j = 0; j < xValues.length; j++) {
            String x = xValues[j];
            String newURL = addParametersToURL(url, inputID, x, selectIDs);
            System.out.println(newURL);
            final HtmlPage page = (HtmlPage) webClient.getPage(newURL);
            List<HtmlElement> outputTags = page.getByXPath("//*[contains(lower-case(text()),'" + yValues[j].toLowerCase() + "')]");
            fillOutputFields(outputTags, yValues[j]);
        }
        String bestOutputField = WrapperUtilities.getPathWithMaxValue(
                outputFieldWittnesses, outputFieldLength);
        if (outputFieldWittnesses.get(bestOutputField) > 1)
            return bestOutputField;

        return null;

    }

    private String findOutputFieldPOST(String inputID,
                                       Map<String, String> selectIDs,
                                       ArrayList<NameValuePair> submitOption, Map<String, String> radioIDs)
            throws FailingHttpStatusCodeException, MalformedURLException,
            IOException, ParserConfigurationException, SAXException,
            XPathExpressionException {
        for (int j = 0; j < xValues.length; j++) {
            String x = xValues[j];
            WebRequest requestSettings = createWebRequest(url, inputID, x,
                    selectIDs);
            // Set the request parameters
            for (String key : radioIDs.keySet()) {
                requestSettings.getRequestParameters().add(
                        new NameValuePair(key, radioIDs.get(key)));
            }
            for (NameValuePair pair : submitOption) {
                requestSettings.getRequestParameters().add(pair);
            }
            final HtmlPage page = (HtmlPage) webClient.getPage(requestSettings);
            if (!page.asText().toLowerCase().contains(yValues[j].toLowerCase())) {
                return null;
            }
            List<HtmlElement> outputTags = page.getByXPath("//*[contains(lower-case(text()),'"
                    + yValues[j].toLowerCase() + "')]");

            if (outputTags.isEmpty()) {
                // org.jsoup.nodes.Document document =
                // Jsoup.parse(page.asXml());
                // XPath xPath = XPathFactory.newInstance().newXPath();
                // Elements email = document.select("b:contains("
                // + yValues[j].toLowerCase() + ")");

                String pageText = page.asXml().toLowerCase();
                // pageText= pageText.replaceAll("<br/>", "");
                pageText = pageText.replaceAll("\t", "");
                pageText = pageText.replaceAll("  ", "");
                int index = pageText.indexOf(yValues[j].toLowerCase());
                int indexOftagBegin = pageText.indexOf(">");
                int indexOftagEnd = pageText.indexOf(">");
                int tagsCount = 0;
                while (indexOftagEnd <= index) {
                    indexOftagBegin = indexOftagEnd;
                    indexOftagEnd = pageText.indexOf(">", indexOftagBegin + 1);
                    ++tagsCount;
                }
                indexOftagEnd = pageText.indexOf("<", indexOftagBegin + 1);
                System.out.println(pageText.substring(indexOftagBegin + 1,
                        indexOftagEnd).replace("\n", ""));

            } else {
                fillOutputFields(outputTags, yValues[j]);
                String bestOutputField = WrapperUtilities.getPathWithMaxValue(
                        outputFieldWittnesses, outputFieldLength);
                if (outputFieldWittnesses.get(bestOutputField) > 1)
                    return bestOutputField;
            }
        }

        // TODO store outputfield value

        return null;
    }

    private boolean findRelevantFieldsJS(HtmlPage page)
            throws FailingHttpStatusCodeException, IOException {
        return false;
    }

    private boolean findRelevantGetFields(HtmlPage page, HtmlElement form)
            throws FailingHttpStatusCodeException, MalformedURLException,
            IOException {

        Set<String> inputIDs = new HashSet<String>();
        Map<String, String> selectIDs = extractSelectionIDs(form);

        // input methods
        List<HtmlElement> inputTags = form.getByXPath(form.getCanonicalXPath() + "//input");
        for (HtmlElement tag : inputTags) {
            if (tag.getAttribute("type").equals("text")
                    || tag.getAttribute("type").equals("")) {
                inputIDs.add(tag.getAttribute("name"));
            }
            if (tag.getAttribute("type").equals("hidden")) {
                selectIDs.put(tag.getAttribute("value"),
                        tag.getAttribute("name"));
            }
        }

        for (String inputID : inputIDs) {
            outputFieldWittnesses.clear();
            String outputPath = findOutputFieldGET(inputID, selectIDs);
            if (outputPath == null) {
                System.out.println("No output found");
            } else {
                wrapper = new HttpFormWrapper("get", url, inputID, outputPath,
                        selectIDs, outputStartingIndex.get(outputPath),
                        outputFieldWittnesses);
                System.out.println(wrapper.toString());
                return true;
            }
        }

        return false;
    }

    public HttpFormWrapper getFormWrapperWith(String formID, String inputID,
                                              Map<String, String> selectIDs, String originalUrl) {
        try {
            final HtmlPage page = (HtmlPage) webClient.getPage(originalUrl);

            String form = getForms().get(formID);
            List<HtmlElement> formHTMl = page.getByXPath(form);
            String method = formHTMl.get(0).getAttribute("method");
            url = createRequestURL(formHTMl.get(0));
            String outputPath = null;
            if (method.isEmpty() || method.toLowerCase().equals("get")) {
                outputPath = findOutputFieldGET(inputID, selectIDs);
                if (outputPath == null) {
                    System.out.println("No output found");
                } else {
                    wrapper = new HttpFormWrapper("get", url, inputID, outputPath,
                            selectIDs, outputStartingIndex.get(outputPath),
                            outputFieldWittnesses);
                    System.out.println(wrapper.toString());
                    return wrapper;
                }
            }

            if (method.isEmpty() || method.toLowerCase().equals("post")) {
                outputPath = findOutputFieldPOST(inputID, selectIDs,
                        new ArrayList<NameValuePair>(),
                        new HashMap<String, String>());
                if (outputPath == null) {
                    System.out.println("No output found");
                } else {
                    wrapper = new HttpFormWrapper("post", url, inputID, outputPath,
                            selectIDs, outputStartingIndex.get(outputPath),
                            outputFieldWittnesses);
                    System.out.println(wrapper.toString());
                    return wrapper;
                }
            }


        } catch (ClassCastException | FailingHttpStatusCodeException
                | IOException | XPathExpressionException
                | ParserConfigurationException | SAXException e) {
            e.printStackTrace();
        }
        return wrapper;
    }

    private boolean findRelevantPostFields(HtmlPage page, HtmlElement form)
            throws FailingHttpStatusCodeException, MalformedURLException,
            IOException {
        // input methods
        Set<String> inputIDs = new HashSet<String>();
        Map<String, String> selectIDs = extractSelectionIDs(form);
        Map<String, String> submitIDs = new HashMap<String, String>();
        Map<String, String> radioIDs = new HashMap<String, String>();
        List<HtmlElement> inputTags = page.getByXPath("//input");
        for (HtmlElement tag : inputTags) {
            String tagType = tag.getAttribute("type");
            if (tagType.equals("text")) {
                inputIDs.add(tag.getAttribute("name"));
            }
            if (tagType.equals("hidden")) {
                selectIDs.put(tag.getAttribute("value"),
                        tag.getAttribute("name"));
            }
            if (tagType.equals("submit")) {
                submitIDs.put(tag.getAttribute("value"),
                        tag.getAttribute("name"));
            }
            if (tagType.equals("radio")) {
                String radioName = tag.getAttribute("name");// TODO make radio
                // bottom multiple
                // options
                if (!radioIDs.containsKey(radioName)) {
                    radioIDs.put(radioName, tag.getAttribute("value"));
                } else {
                    if (tag.getAttribute("value").contains(inputColumn)) {
                        radioIDs.put(radioName, tag.getAttribute("value"));
                    }
                }
            }
        }

        inputTags = form.getByXPath("//textarea");
        for (HtmlElement tag : inputTags) {
            inputIDs.add(tag.getAttribute("name"));
        }
        if (inputIDs.isEmpty()) {

        }
        // webClient.getOptions().setJavaScriptEnabled(true);
        List<ArrayList<NameValuePair>> submitOptions = combineRadioAndSubmitFields(
                submitIDs, radioIDs);
        for (String inputID : inputIDs) {
            for (ArrayList<NameValuePair> submitOption : submitOptions) {
                outputFieldWittnesses.clear();
                String outputPath = null;
                try {
                    outputPath = findOutputFieldPOST(inputID, selectIDs,
                            submitOption, radioIDs);
                } catch (XPathExpressionException
                        | ParserConfigurationException | SAXException e) {
                    e.printStackTrace();
                }

                if (outputPath == null) {
                    System.out.println("No output found");
                } else {
                    wrapper = new HttpFormWrapper("post", url, inputID,
                            outputPath, selectIDs,
                            outputStartingIndex.get(outputPath),
                            outputFieldWittnesses);
                    System.out.println(wrapper.toString());
                    return true;
                }
            }
        }
        return false;
    }

    public String getInputColumn() {
        return inputColumn;
    }

    public String getOutputColumn() {
        return outputColumn;
    }

    public String getUrl() {
        return url;
    }

    public String[] getxValues() {
        return xValues;
    }

    public String[] getyValues() {
        return yValues;
    }

    public void setInputColumn(String inputColumn) {
        this.inputColumn = inputColumn;
    }

    public void setOutputColumn(String outputColumn) {
        this.outputColumn = outputColumn;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setxValues(String[] xValues) {
        this.xValues = xValues;
    }

    public void setyValues(String[] yValues) {
        this.yValues = yValues;
    }

    public String[] transformValues(String[] xValues)
            throws FailingHttpStatusCodeException, MalformedURLException,
            IOException {
        if (wrapper == null) {
            return null;
        } else if (wrapper.getWrapperType().equals("post")) {
            return transformValuesPOST(xValues);
        }
        return transformValuesGET(xValues);
    }

    private String[] transformValuesGET(String[] xValues)
            throws FailingHttpStatusCodeException, MalformedURLException,
            IOException {
        String[] ys = new String[xValues.length];
        for (int i = 0; i < xValues.length; i++) {
            String newURL = addParametersToURL(wrapper.getUrl(),
                    wrapper.getInputParam(), xValues[i], wrapper.getSelectIDs());
            final HtmlPage page = (HtmlPage) webClient.getPage(newURL);
            HtmlElement output = (HtmlElement) page.getFirstByXPath(wrapper
                    .getOutputpath());
            ys[i] = output.asText();
            System.out.println(ys[i]);
        }
        return ys;
    }

    private String[] transformValuesPOST(String[] xValues)
            throws FailingHttpStatusCodeException, MalformedURLException,
            IOException {
        String[] ys = new String[xValues.length];
        for (int i = 0; i < xValues.length; i++) {
            WebRequest requestSettings = createWebRequest(wrapper.getUrl(),
                    wrapper.getInputParam(), xValues[i], wrapper.getSelectIDs());
            final HtmlPage page = (HtmlPage) webClient.getPage(requestSettings);
            HtmlElement output = (HtmlElement) page.getFirstByXPath(wrapper
                    .getOutputpath());
            ys[i] = output.asText();
            System.out.println(ys[i]);
        }
        return ys;

    }

}
