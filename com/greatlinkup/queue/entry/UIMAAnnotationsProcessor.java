/*
 * 
 */
package com.greatlinkup.queue.entry;

import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.htmlcleaner.ContentToken;
import org.htmlcleaner.TagNode;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author lcahlander
 *
 */
public class UIMAAnnotationsProcessor extends Thread {
	private EntryProcessor entryProcessor = null;
	private String bodyText = null;
	private String partsOfSpeechURL = null;
	private Map<String,Integer> tokenCountMap = new HashMap<String,Integer>();
	private Map<String,Integer> stemCountMap = new HashMap<String,Integer>();

	public UIMAAnnotationsProcessor(EntryProcessor entryProcessor,
			String bodyText, String partsOfSpeechURL) {
		this.entryProcessor = entryProcessor;
		this.bodyText = bodyText;
		this.partsOfSpeechURL = partsOfSpeechURL;
	}

	public void run() {
		TagNode result = null;
		try {
	        // Construct data
	        String data = URLEncoder.encode("text", "UTF-8") + "=" + URLEncoder.encode(bodyText, "UTF-8");
	        data += "&" + URLEncoder.encode("mode", "UTF-8") + "=" + URLEncoder.encode("xml", "UTF-8");
	    
	        // Send data
	        URL url = new URL((partsOfSpeechURL != null) ? partsOfSpeechURL : "http://localhost:8080/parts-of-speech/simple-server-test");
	        URLConnection conn = url.openConnection();
	        conn.setDoOutput(true);
	        OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
	        wr.write(data);
	        wr.flush();
	        
	        // Get the response
	        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
	        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
	        Document doc = documentBuilder.parse(conn.getInputStream());
	        result = walkNode(doc.getDocumentElement());
	        TagNode tokenCounts = new TagNode("TokenCounts");
	        result.addChild(tokenCounts);
	        Iterator<Entry<String, Integer>> iterator = tokenCountMap.entrySet().iterator();
	        while (iterator.hasNext()) {
	        	Entry<String, Integer> entry = iterator.next();
	        	String[] keyElements = entry.getKey().split("~");
	        	TagNode tagNode = new TagNode("TokenCount");
	        	tagNode.addAttribute("posTag", keyElements[keyElements.length - 1]);
	        	tagNode.addAttribute("count", entry.getValue().toString());
				tagNode.addChild(new ContentToken(keyElements[0]));
				tokenCounts.addChild(tagNode);
	        }
	        tokenCountMap = new HashMap<String, Integer>();
	        TagNode stemCounts = new TagNode("StemCounts");
	        result.addChild(stemCounts);
	        iterator = stemCountMap.entrySet().iterator();
	        while (iterator.hasNext()) {
	        	Entry<String, Integer> entry = iterator.next();
	        	String[] keyElements = entry.getKey().split("~");
	        	TagNode tagNode = new TagNode("StemCount");
	        	tagNode.addAttribute("posTag", keyElements[keyElements.length - 1]);
	        	tagNode.addAttribute("count", entry.getValue().toString());
				tagNode.addChild(new ContentToken(keyElements[0]));
				stemCounts.addChild(tagNode);
	        }
	        stemCountMap = new HashMap<String, Integer>();
	        wr.close();
	    } catch (Exception e) {
	    } finally {
	        entryProcessor.setUimaAnnotationsResult(result);
	    }
	}
	private TagNode walkNode(Node domNode) {
	    NodeList children = domNode.getChildNodes();
	    TagNode tagNode = printNode(domNode);
	    for (int i = 0; i < children.getLength(); i++) {
	      Node childDomNode = children.item(i);
	      System.out.println(childDomNode.getNodeName() + ":" + childDomNode.getNodeType() + ":" + childDomNode.getNodeValue());
	      if (childDomNode.getNodeName().equalsIgnoreCase("#text")) {
	    	  if ((childDomNode.getNodeValue() != null) && (childDomNode.getNodeValue().trim().length() > 0)) {
					tagNode.addChild(new ContentToken(childDomNode.getNodeValue()));
	    	  }
	      } else if (childDomNode.hasChildNodes())
	        tagNode.addChild(walkNode(childDomNode));
	      else {
		        TagNode zNode = printNode(childDomNode);
		        tagNode.addChild(zNode);
	      }
	    }
	    return tagNode;
	  }
	  private TagNode printNode(Node domNode) {
		  if (domNode.getNodeName().equals("TokenAnnotation")) {
		      System.out.println(domNode.getNodeName() + ":" + domNode.getNodeType() + ":" + domNode.getNodeValue());
		      String posTag = "";
		      String stem = "";
			  if (domNode.hasAttributes()) {
				  NamedNodeMap map = domNode.getAttributes();
				  int size = map.getLength();
				  for (int index = 0; index < size; index++) {
					  Node node = map.item(index);
					  String key = node.getNodeName();
					  if (key.equals("posTag")) {
						  posTag = node.getNodeValue();
					  } else if (key.equalsIgnoreCase("stem")) {
						  stem = node.getNodeValue();
					  }
				  }
				  String mapKey = stem + "~" + posTag;
				  Integer count = stemCountMap.get(mapKey);
				  if (count == null) {
					  count = new Integer(1);
				  } else {
					  count = new Integer(count.intValue() + 1);
				  }
				  stemCountMap.put(mapKey, count);

			  }
			  NodeList children = domNode.getChildNodes();
			  for (int index = 0; index < children.getLength(); index++) {
				  Node childDomNode = children.item(index);
				  if (childDomNode.getNodeName().equalsIgnoreCase("#text")) {
					  String token = childDomNode.getNodeValue();
					  String mapKey = token + "~" + posTag;
					  Integer count = tokenCountMap.get(mapKey);
					  if (count == null) {
						  count = new Integer(1);
					  } else {
						  count = new Integer(count.intValue() + 1);
					  }
					  tokenCountMap.put(mapKey, count);
				  }
			  }
		  }
		  TagNode returnNode = new TagNode(domNode.getNodeName());
			if (domNode.getNodeValue() != null) {
				returnNode.addChild(new ContentToken(domNode.getNodeValue()));
			}
		  if (domNode.hasAttributes()) {
			  NamedNodeMap map = domNode.getAttributes();
			  int size = map.getLength();
			  for (int index = 0; index < size; index++) {
				  Node node = map.item(index);
				  returnNode.addAttribute(node.getNodeName(), node.getNodeValue());
			  }
		  }
		return returnNode;
	  }
}
