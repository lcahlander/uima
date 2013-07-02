/**
 * 
 */
package com.greatlinkup.queue.entry;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

import org.apache.commons.lang.StringEscapeUtils;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;

/**
 * @author lcahlander
 *
 */
public class GreatlinkupAbstractProcessor extends Thread {

	private EntryProcessor entryProcessor = null;
	private String link = null;
	private HtmlCleaner cleaner = null;
	
	public GreatlinkupAbstractProcessor(EntryProcessor entryProcessor, String link, HtmlCleaner cleaner) {
		this.entryProcessor = entryProcessor;
		this.link = link;
		this.cleaner = cleaner;
	}
	
	public void run() {
		TagNode sofa = new TagNode(EntryProcessor.EXIST_SOFA_ELEMENT, cleaner);
		sofa.addChild(HTMLCleanerUtil.newTextNode(cleaner, EntryProcessor.EXIST_SOFA_NAME, "Abstract"));
		String base = "http://ws.greatlinkup.com/getAbstractDCMI.aspx";
		String param0= "";
		try {
			String linkClone = link;
			param0 = URLEncoder.encode(linkClone, "UTF-8");
			URL url = new URL(base + "?u=" + param0);
			TagNode htmlNode = cleaner.clean(url);
			TagNode description = htmlNode.findElementByName("Description", true);
			TagNode body = new TagNode(EntryProcessor.EXIST_SOFA_VALUE, cleaner);
			if (description != null) {
				description.removeAttribute("about");
				body.addChild(description);
			}
			sofa.addChild(body);
		} catch (Exception e) {
			TagNode error = HTMLCleanerUtil.newTextNode(cleaner, "ERROR", e.getLocalizedMessage());
			sofa.addChild(error);
		} finally {
			entryProcessor.setGreatlinkupAbstractSofa(sofa);
		}

	}
}
