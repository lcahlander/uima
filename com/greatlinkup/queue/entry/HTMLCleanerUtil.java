/**
 * 
 */
package com.greatlinkup.queue.entry;

import java.util.Iterator;
import java.util.List;

import org.htmlcleaner.ContentToken;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;

/**
 * @author lcahlander
 *
 */
public class HTMLCleanerUtil {

	public static void replaceAll(StringBuffer stringBuffer, String searchText, String replaceText) {
		while (stringBuffer.indexOf(searchText) >= 0) {
			int index = stringBuffer.indexOf(searchText);
			stringBuffer.replace(index, index + searchText.length(), replaceText);
		}
	}

	public static String getBodyText(HtmlCleaner cleaner, TagNode htmlNode, TagNode metaHeadersNode) {
		pruneDiv(htmlNode, "ibm-navigation");
		pruneDiv(htmlNode, "ibm-masthead");
		pruneDiv(htmlNode, "ibm-footer");
		pruneDiv(htmlNode, "ibm-page-tools");
		pruneDiv(htmlNode, "ibm-access");
		pruneDiv(htmlNode, "no-print");
		pruneDiv(htmlNode, "ibm-contact-module");
		pruneDiv(htmlNode, "ibm-merchandising-module");
		processHeaderMetadata(cleaner, htmlNode, metaHeadersNode);
		StringBuffer bodyText = htmlNode.getText();
		HTMLCleanerUtil.replaceAll(bodyText, "&amp;", "&");
		HTMLCleanerUtil.replaceAll(bodyText, "&nbsp;", " ");
		HTMLCleanerUtil.replaceAll(bodyText, "&gt;", ">");
		HTMLCleanerUtil.replaceAll(bodyText, "&lt;", "<");
		return bodyText.toString();
	}

	private static void processHeaderMetadata(HtmlCleaner cleaner, TagNode htmlNode, TagNode metaHeadersNode) {
		List children = htmlNode.getChildren();
		Iterator iterator = children.iterator();
		while (iterator.hasNext()) {
			Object child = iterator.next();

			if (child instanceof TagNode) {
				TagNode childNode = (TagNode) child;
				String childName = childNode.getName();
				if (childName.equalsIgnoreCase("meta")) {
					String name = childNode.getAttributeByName("name");
					String content = childNode.getAttributeByName("content");
					TagNode metaHeaderNode = new TagNode("MetaHeader");
					metaHeaderNode.addChild(newTextNode(cleaner, "Name", name));
					metaHeaderNode.addChild(newTextNode(cleaner, "Content", content));
					metaHeadersNode.addChild(metaHeaderNode);
				} else {
					processHeaderMetadata(cleaner, childNode, metaHeadersNode);
				}
			}
		}
	}

	private static void pruneDiv(TagNode htmlNode, String string) {
		List children = htmlNode.getChildren();
		Iterator iterator = children.iterator();
		while (iterator.hasNext()) {
			Object child = iterator.next();

			if (child instanceof TagNode) {
				TagNode childNode = (TagNode) child;
				String childName = childNode.getName();
				if (childName.equalsIgnoreCase("div")) {
					String idString = childNode.getAttributeByName("id");
					String classString = childNode.getAttributeByName("class");
					if (idString != null && idString.equalsIgnoreCase(string)) {
						htmlNode.removeChild(childNode);
						return;
					} else if (classString != null
							&& classString.equalsIgnoreCase(string)) {
						htmlNode.removeChild(childNode);
						return;
					} else {
						pruneDiv(childNode, string);
					}
				} else {
					pruneDiv(childNode, string);
				}
			}
		}
	}
	public static TagNode newTextNode(HtmlCleaner cleaner, String elementName, String text) {
		TagNode node = new TagNode(elementName, cleaner);
		if (text != null) {
			node.addChild(new ContentToken(text));
		}
		return node;
	}
}
