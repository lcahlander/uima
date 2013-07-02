/**
 * 
 */
package com.greatlinkup.queue.entry;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.htmlcleaner.ContentToken;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;

/**
 * @author lcahlander
 *
 */
public class GreatlinkupWeightedAbstractProcessor extends Thread {
	private EntryProcessor entryProcessor = null;
	private String link = null;
	private HtmlCleaner cleaner = null;
	
	public GreatlinkupWeightedAbstractProcessor(EntryProcessor entryProcessor, String link, HtmlCleaner cleaner) {
		this.entryProcessor = entryProcessor;
		this.link = link;
		this.cleaner = cleaner;
	}
	
	public void run() {
		TagNode sofa = new TagNode(EntryProcessor.EXIST_SOFA_ELEMENT, cleaner);
		sofa.addChild(HTMLCleanerUtil.newTextNode(cleaner, EntryProcessor.EXIST_SOFA_NAME, "WeightedAbstract"));
		String base = "http://ws.greatlinkup.com/getWeightedAbstract.aspx";
		String param0= "";
		try {
			String linkClone = link;
			param0 = URLEncoder.encode(linkClone, "UTF-8");
			URL url = new URL(base + "?u=" + param0 + "&l=");
			TagNode htmlNode = cleaner.clean(url);
			TagNode description = htmlNode.findElementByName("abstract", true);
			TagNode body = new TagNode(EntryProcessor.EXIST_SOFA_VALUE, cleaner);
			TagNode elements = new TagNode("abstractElements", cleaner);
			body.addChild(elements);
			if (description != null) {
//				htmlNode.removeAttribute("about");
				TagNode[] children = description.getChildTags();
				for (int index = 0; index < children.length; index++) {
					TagNode abstractElement = children[index];
					TagNode abstractLevel = abstractElement.findElementByName("abstractLevel", true);
					String levelValue = "";
					String phraseValue = "";
					List list = abstractLevel.getChildren();
					Iterator iterator = list.iterator();
					while(iterator.hasNext()) {
						Object obj = iterator.next();
						if (obj instanceof ContentToken) {
							ContentToken node = (ContentToken) obj;
							if (node != null) {
								levelValue = node.getContent();
							}
						}
					}
					
					TagNode abstractPhrase = abstractElement.findElementByName("abstractPhrase", true);
					list = abstractPhrase.getChildren();
					iterator = list.iterator();
					while(iterator.hasNext()) {
						Object obj = iterator.next();
						if (obj instanceof ContentToken) {
							ContentToken node = (ContentToken) obj;
							if (node != null) {
								phraseValue = node.getContent();
							}
						}
					}
					
					TagNode element = new TagNode("abstractElement", cleaner);
					TagNode level = HTMLCleanerUtil.newTextNode(cleaner, "abstractLevel", levelValue);
					TagNode phrase = HTMLCleanerUtil.newTextNode(cleaner, "abstractPhrase", phraseValue);
					element.addChild(level);
					element.addChild(phrase);
					elements.addChild(element);
					
//					System.out.println(abstractPhrase.getText());
				}
//				body.addChild(description);
			}
			sofa.addChild(body);
		} catch (Exception e) {
			TagNode error = HTMLCleanerUtil.newTextNode(cleaner, "ERROR", e.getLocalizedMessage());
			sofa.addChild(error);
		} finally {
			entryProcessor.setGreatlinkupWeightedAbstractSofa(sofa);
		}

	}
}
