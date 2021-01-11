package webforms.wrappers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.ScriptResult;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlButtonInput;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlForm;
import com.gargoylesoftware.htmlunit.html.HtmlInput;
import com.gargoylesoftware.htmlunit.html.HtmlOption;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlScript;
import com.gargoylesoftware.htmlunit.html.HtmlSelect;
import com.gargoylesoftware.htmlunit.html.HtmlTextInput;

public class JavaScriptWrapper {

	final static WebClient webClient = new WebClient(BrowserVersion.getDefault());

	public static void main(String[] args)
			throws FailingHttpStatusCodeException, MalformedURLException,
			IOException {
		String url = "http://www.onlineconversion.com/area.html";

		webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setJavaScriptEnabled(true);
		final HtmlPage page = (HtmlPage) webClient.getPage(url);

		List<HtmlElement> tags = page.getByXPath("//input");
		List<String> jsFunctions = new ArrayList<String>();
		for (HtmlElement tag : tags) {
			if (tag.hasAttribute("onClick")) {
				jsFunctions.add(tag.getAttribute("onClick"));
			}
		}
		for (String function : jsFunctions) {
			System.out.println(function);
		}
		JavaScriptWrapper jsw = new JavaScriptWrapper();
		List<String> jsFileURIs = jsw.collectJSFileURIs(page);
		Map<String, String> jsFunction2SourceFiles = jsw.findFunctions(
				jsFileURIs, jsFunctions);

	}

	private  Map<String, String> findFunctions(
			List<String> jsFileURIs, List<String> jsFunctions)
			throws FailingHttpStatusCodeException, MalformedURLException,
			IOException {
		Map<String, String[]> map = new HashMap<String, String[]>();
		for (String url : jsFileURIs) {
			URL oracle = new URL(url);
			URLConnection yc = oracle.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(
					yc.getInputStream()));
			String inputLine;
			StringBuilder fileTextBuffer = new StringBuilder();
			while ((inputLine = in.readLine()) != null) {
				fileTextBuffer.append(inputLine).append("\n");
			}
			String filetext = fileTextBuffer.toString();
			in.close();

			for (String functString : jsFunctions) {
				String funcName = functString.replace("javascript:", "")
						.replace(";", "");

				if (filetext.contains("function" + " " + funcName)) {
					int beginIndex = filetext.indexOf("function" + " "
							+ funcName);
					int endIndex = filetext.indexOf("\n}", beginIndex);
					String functionDef = filetext.substring(beginIndex,
							endIndex + 1);
					String[] params = findRelevantVariables(functionDef);

					map.put(functString, params);
					System.out.println();
				}
			}
			if (jsFunctions.size()== map.size()) {
				System.out.println("All functions found");
				break;
			}
		}
		return null;
	}

	public void wrapJavaScriptFile() {

	}

	public List<String> collectJSFileURIs(HtmlPage page) {
		String pageAsXml = page.asXml();
		List<String> srcURIStrings = new ArrayList<String>();
		List<HtmlScript> scriptElements = page.getByXPath("//script");

		String baseUrl = page.getUrl().toString()
				.replace(page.getUrl().getPath(), "/");

		for (HtmlScript scriptElement : scriptElements) {
			if (scriptElement.getAttribute("language").toLowerCase()
					.equals("javascript")
					&& scriptElement.hasAttribute("src")) {
				String srcURI = scriptElement.getAttribute("src");
				if (!srcURI.contains("http")) {
					if (srcURI.startsWith("/")) {
						srcURI = srcURI.substring(1);
					}
					srcURI = baseUrl + srcURI;
				}
				System.out.println(srcURI);
				srcURIStrings.add(srcURI);
			}
		}

		return srcURIStrings;
	}


	private String[] findRelevantVariables(String functionDef) {
		// TODO parse parameters
		int pointer = 0;
		Set<String> params = new HashSet<String>();
		while (pointer > -1) {
			pointer = functionDef.indexOf("document.", pointer);
			if (pointer == -1) {
				break;
			}
			pointer = functionDef.indexOf(".", pointer
					+ "document.".length());
			if (pointer == -1) {
				break;
			}
			int endpointer = functionDef
					.indexOf(".", pointer+1);
			if (functionDef.startsWith("elements", pointer + 1)) {
				// TODO read field value
			} else {
				String paramString = functionDef.substring(
						pointer + 1, endpointer);
				if (paramString.contains("[")) {
					paramString = paramString.substring(0, paramString.indexOf("["));
				}
				System.out.println(paramString);
				params.add(paramString);
			}
			pointer = endpointer+1;
		}
		return params.toArray(new String[params.size()]);
	}

	private List<HtmlInput> findInputField(String[] inputNames) {
		return null;// TODO
	}

	public static void exampleJSExecution()
			throws FailingHttpStatusCodeException, MalformedURLException,
			IOException {
		String url = "http://www.onlineconversion.com/area.html";
		final WebClient webClient = new WebClient(BrowserVersion.getDefault());

		webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setJavaScriptEnabled(true);
		final HtmlPage page = (HtmlPage) webClient.getPage(url);
		final HtmlForm form = page.getFormByName("MainForm");
		final HtmlTextInput textField = form.getInputByName("what");
		final HtmlButtonInput button = form.getInputByName("Go");
		final HtmlSelect to = form.getSelectByName("to");

		HtmlOption option = to.getOptionByText("barn");
		to.setSelectedAttribute(option, true);

		textField.setValueAttribute("5");
		String javaScriptCode = "myCon();";
		// final HtmlPage page2 = button.click();

		ScriptResult result = page.executeJavaScript(javaScriptCode);
		result.getJavaScriptResult();
		HtmlPage page2 = (HtmlPage) result.getNewPage();
		System.out.println(page2.getFormByName("MainForm")
				.getInputByName("answer").asText());
		// System.out.println("result: "+
		// result.getNewPage().getWebResponse().get);
	}

}
