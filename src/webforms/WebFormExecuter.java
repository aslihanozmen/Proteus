package webforms;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;

import webforms.wrappers.HttpFormWrapper;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.HttpMethod;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.util.NameValuePair;

public class WebFormExecuter {
	
	private HttpFormWrapper wrapper;

	public WebFormExecuter(HttpFormWrapper wrapper) {
		this.wrapper = wrapper;
	}
	
	private final WebClient webClient = new WebClient(
			BrowserVersion.INTERNET_EXPLORER);
	
	public String[] transformValues(String [] xValues) throws FailingHttpStatusCodeException, MalformedURLException, IOException {
		webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setJavaScriptEnabled(false);
		if (wrapper == null) {
			return null;
		}else if (wrapper.getWrapperType().equals("post")) {
			return transformValuesPOST(xValues);
		}
		return transformValuesGET(xValues);
	}
	
	
	private String[] transformValuesGET(String[] xValues)
			throws FailingHttpStatusCodeException, MalformedURLException,
			IOException {
		String[] ys = new String[xValues.length];
		for (int i=0;i< xValues.length;i++) {
			String newURL = addParametersToURL(wrapper.getUrl(),
					wrapper.getInputParam(), xValues[i], wrapper.getSelectIDs());
			final HtmlPage page = (HtmlPage) webClient.getPage(newURL);
			HtmlElement output = (HtmlElement) page.getFirstByXPath(wrapper.getOutputpath());
			if (output!= null)
				ys[i] = output.asText().substring(wrapper.getOutputStartingIndex());
			else {
				for (String path : wrapper.getOutputpathOptions().keySet()) {
					output = (HtmlElement) page.getFirstByXPath(path);
					if (output!= null){
						ys[i] = output.asText();
						break;
					}
				}
			}
			if (output==null) {
				ys[i] = "";
			}
			System.out.println(ys[i]);
		}
		return ys;
	}

	private String[] transformValuesPOST(String[] xValues) throws FailingHttpStatusCodeException,
			MalformedURLException, IOException {
		String[] ys = new String[xValues.length];
		for (int i=0;i< xValues.length;i++) {
			WebRequest requestSettings = createWebRequest(wrapper.getUrl(), wrapper.getInputParam(), xValues[i],
					wrapper.getSelectIDs());
			final HtmlPage page = (HtmlPage) webClient.getPage(requestSettings);
			HtmlElement output = (HtmlElement) page.getFirstByXPath(wrapper.getOutputpath());
			if (output!= null)
				ys[i] = output.asText().substring(wrapper.getOutputStartingIndex());
			else {
				for (String path : wrapper.getOutputpathOptions().keySet()) {
					output = (HtmlElement) page.getFirstByXPath(path);
					if (output!= null){
						ys[i] = output.asText().substring(wrapper.getOutputStartingIndex());
						break;
					}
				}
			}
			if (output==null) {
				ys[i] = "";
			}
			System.out.println(ys[i]);
		}
		return ys;

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

}
