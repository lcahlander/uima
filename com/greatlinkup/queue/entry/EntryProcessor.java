/*
 * 
 */
package com.greatlinkup.queue.entry;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.htmlcleaner.CleanerProperties;
import org.htmlcleaner.CleanerTransformations;
import org.htmlcleaner.DomSerializer;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.PrettyXmlSerializer;
import org.htmlcleaner.TagNode;
import org.htmlcleaner.TagTransformation;
import org.w3c.dom.Document;
import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.base.XMLDBException;
import org.xmldb.api.modules.XMLResource;

import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.greatlinkup.queue.QueueProcessingJavaJob;
import com.greatlinkup.queue.QueueUtility;
import com.greatlinkup.queue.entry.HTMLCleanerUtil;
import com.greatlinkup.queue.entry.GreatlinkupAbstractProcessor;
import com.greatlinkup.queue.entry.GreatlinkupWeightedAbstractProcessor;
import com.greatlinkup.queue.entry.GreatlinkupWeightedIndexProcessor;
import com.greatlinkup.queue.entry.UIMAAnnotationsProcessor;

/**
 * @author lcahlander
 *
 */
public class EntryProcessor implements Runnable {

	/** The Constant logger. */
	protected final static Logger logger = Logger.getLogger(EntryProcessor.class);

	public static final String EXIST_DOCUMENT_ROOT = "SyndEntryDocument";

	public static final String EXIST_HEADER_ELEMENT = "SyndFeed";

	public static final String EXIST_HEADER_TITLE = "SyndFeedTitleText";

	public static final String EXIST_HEADER_DESCRIPTION = "SyndFeedDescriptionText";

	public static final String EXIST_HEADER_FEED_TYPE = "SyndFeedTypeCode";

	public static final String EXIST_HEADER_LINK = "SyndFeedURI";

	public static final String EXIST_ENTRY_ELEMENT = "SyndEntry";

	public static final String EXIST_ENTRY_TITLE = "SyndEntryTitleText";

	public static final String EXIST_ENTRY_DESCRIPTION = "SyndEntryDescriptionText";

	public static final String EXIST_ENTRY_AUTHOR = "SyndEntryAuthorText";

	public static final String EXIST_ENTRY_LINK = "SyndEntryURI";

	public static final String EXIST_ENTRY_PUBLISHED = "SyndEntryPublishedDateTime";

	public static final String EXIST_ENTRY_UPDATED = "SyndEntryUpdateDateTime";

	/**
	 * This is the DateTime that the SyndEntry was processed and stored into the
	 * local datastore. It is stored with GMT timezone.
	 */
	public static final String EXIST_ENTRY_PROCESSED = "LastProcessedDateTime";

	public static final String EXIST_SOFA_CONTAINER = "Sofas";

	public static final String EXIST_SOFA_ELEMENT = "Sofa";

	public static final String EXIST_SOFA_NAME = "SofaName";

	public static final String EXIST_SOFA_VALUE = "SofaBodyText";

	public static final String EXIST_SOFA_ANNOTATION_CONTAINER = "Annotations";

	public static final String EXIST_SOFA_ANNOTATION = "Annotation";

	public static final String EXIST_SOFA_ANNOTATION_NAME = "AnnotationName";

	public static final String EXIST_SOFA_ANNOTATION_VALUE = "AnnotationCoveredText";

	public static final String EXIST_ANNOTATION_ATTRIBUTE_CONTAINER = "AnnotationAttributes";

	public static final String EXIST_ANNOTATION_ATTRIBUTE = "AnnotationAttribute";

	public static final String EXIST_ANNOTATION_ATTRIBUTE_NAME = "Name";

	public static final String EXIST_ANNOTATION_ATTRIBUTE_VALUE = "Value";
	
	public static final String UTC_FORMAT = "yyyy-MM-dd hh:mm:ss";
	
	public static final String GMT = "GMT";
	
	private TagNode uimaAnnotationsResult;

	private boolean uimaAnnotationsDone;

	private TagNode greatlinkupWeightedAbstractSofa;

	private boolean greatlinkupWeightedAbstractDone;

	private TagNode greatlinkupAbstractSofa;

	private boolean greatlinkupAbstractDone;

	private TagNode greatlinkupWeightedIndexSofa;

	private boolean greatlinkupWeightedIndexDone;

	private HtmlCleaner cleaner;

	private DomSerializer serializer;

	private PrettyXmlSerializer prettySerializer;

	private String existURLString;

	private String existUserName;

	private String existCollection;

	private String existUserPassword;

	private SyndFeed syndFeed;

	private SyndEntry syndEntry;

	private TagNode root;

	private String finalURLString;

	private int hopCount;

	private ArrayList<String> redirectList;

	private TagNode headersNode;

	private Object content;

	private String bodyText;

	private TagNode metaHeadersNode;

	private String processCollectionPath;

	public static final String existDriverName = "org.exist.xmldb.DatabaseImpl";

	private String customerID;

	private String partsOfSpeechURL;

	private DomSerializer storeSerializer;

	/**
	 * 
	 * @param inFeed
	 * @param syndEntry
	 * @param parameters
	 */
	public EntryProcessor(SyndFeed inFeed, SyndEntry syndEntry, Map<String, String> parameters) {
		// TODO Auto-generated constructor stub
		logger.info("Creating Runnable EntryProcessor");
		this.syndFeed = inFeed;
		this.syndEntry = syndEntry;
		customerID = parameters.get(QueueProcessingJavaJob.CUSTOMER_ID);
		processCollectionPath = parameters.get(QueueProcessingJavaJob.PROCESS_COLLECTION_PATH);
		existURLString = parameters.get(QueueProcessingJavaJob.EXIST_URL_STRING);
		existUserName = parameters.get(QueueProcessingJavaJob.EXIST_USER_NAME);
		existUserPassword = parameters.get(QueueProcessingJavaJob.EXIST_USER_PASSWORD);
		partsOfSpeechURL = parameters.get(QueueProcessingJavaJob.PARTS_OF_SPEECH_URL);
		if (logger.isInfoEnabled()) {
			logger.info("customerID                       [" + customerID + "]");
			logger.info("existURLString                   [" + existURLString + "]");
			logger.info("existUserName                    [" + existUserName + "]");
			logger.info("processCollectionPath            [" + processCollectionPath + "]");
			logger.info("partsOfSpeechURL                 [" + partsOfSpeechURL + "]");
		}
	}

	/**
	 * 
	 */
	public void run() {
		logger.info("Running EntryProcessor");
		logger.info(syndFeed.getTitle() + " :: " + syndEntry.getTitle());
		initialize();
		process();
	}

	/**
	 * 
	 * @param result
	 */
	public void setUimaAnnotationsResult(TagNode uimaAnnotationsResult) {
		this.uimaAnnotationsResult = uimaAnnotationsResult;
		this.uimaAnnotationsDone = true;
	}

	/**
	 * 
	 * @param sofa
	 */
	public void setGreatlinkupWeightedIndexSofa(TagNode greatlinkupWeightedIndexSofa) {
		this.greatlinkupWeightedIndexSofa = greatlinkupWeightedIndexSofa;
		this.greatlinkupWeightedIndexDone = true;
	}

	/**
	 * 
	 * @param sofa
	 */
	public void setGreatlinkupWeightedAbstractSofa(TagNode greatlinkupWeightedAbstractSofa) {
		this.greatlinkupWeightedAbstractSofa = greatlinkupWeightedAbstractSofa;
		this.greatlinkupWeightedAbstractDone = true;
	}

	/**
	 * 
	 * @param sofa
	 */
	public void setGreatlinkupAbstractSofa(TagNode greatlinkupAbstractSofa) {
		this.greatlinkupAbstractSofa = greatlinkupAbstractSofa;
		this.greatlinkupAbstractDone = true;
	}

	public void initialize() {
		cleaner = new HtmlCleaner();
		CleanerTransformations cleanerTransformations = new CleanerTransformations();

		cleanerTransformations.addTransformation(new TagTransformation("a"));
		cleanerTransformations.addTransformation(new TagTransformation("b"));
		cleanerTransformations.addTransformation(new TagTransformation("i"));
		cleanerTransformations.addTransformation(new TagTransformation("form"));
		cleanerTransformations.addTransformation(new TagTransformation("h1"));
		cleanerTransformations.addTransformation(new TagTransformation("table"));
		cleanerTransformations.addTransformation(new TagTransformation("tr"));
		cleanerTransformations.addTransformation(new TagTransformation("th"));
		cleanerTransformations.addTransformation(new TagTransformation("td"));
		cleanerTransformations.addTransformation(new TagTransformation("tbody"));
		cleanerTransformations.addTransformation(new TagTransformation("thead"));
		cleanerTransformations.addTransformation(new TagTransformation("span"));
		cleanerTransformations.addTransformation(new TagTransformation("br"));
		cleaner.setTransformations(cleanerTransformations);

		CleanerProperties props = cleaner.getProperties();
		CleanerProperties props2 = cleaner.getProperties();
		CleanerProperties props3 = cleaner.getProperties();

		props.setNamespacesAware(false);
		props.setPruneTags("script,style,link,img,noscript,select,option,input,label,ul");
		props.setOmitComments(true);
		props.setAdvancedXmlEscape(false);
		serializer = new DomSerializer(props);

		props2.setNamespacesAware(false);
		props2.setAdvancedXmlEscape(false);
		prettySerializer = new PrettyXmlSerializer(props2);

		props3.setNamespacesAware(false);
		props3.setAdvancedXmlEscape(false);
		storeSerializer = new DomSerializer(props2);
}

	private void process() {
		content = null;
		root = new TagNode(EXIST_DOCUMENT_ROOT, cleaner);
		root.addChild(HTMLCleanerUtil.newTextNode(cleaner, QueueProcessingJavaJob.CUSTOMER_ID, customerID));
		if (syndEntry == null) {
			addSyndFeedToRoot();
		} else {
			finalURLString = syndEntry.getLink();
			hopCount = 0;
			redirectList = new ArrayList<String>();
			headersNode = new TagNode("HTTPHeaders");
			processLink(finalURLString);
			addSyndFeedToRoot();
			addSyndEntryToRoot();
			if (content != null) {
				TagNode sofaContainer = addSofaContainerToRoot();
				TagNode textSofa = getHtmlPageSofa();
				if (textSofa != null) {
					sofaContainer.addChild(textSofa);
					UIMAAnnotationsProcessor annotationsProcessor = new UIMAAnnotationsProcessor(this, bodyText, partsOfSpeechURL);
					GreatlinkupAbstractProcessor abstractProcessor = new GreatlinkupAbstractProcessor(this, finalURLString, cleaner);
					GreatlinkupWeightedAbstractProcessor weightedAbstractProcessor = new GreatlinkupWeightedAbstractProcessor(this, finalURLString, cleaner);
					GreatlinkupWeightedIndexProcessor weightedIndexProcessor = new GreatlinkupWeightedIndexProcessor(this, finalURLString, cleaner);
					annotationsProcessor.run();
					abstractProcessor.run();
					weightedAbstractProcessor.run();
					weightedIndexProcessor.run();

					int counter = 0;
					while ((uimaAnnotationsDone == false || greatlinkupAbstractDone == false || greatlinkupWeightedAbstractDone == false || greatlinkupWeightedIndexDone == false) && (counter++ < 120)) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					if (uimaAnnotationsResult != null) {
						textSofa.addChild(uimaAnnotationsResult);
					} else {
						annotationsProcessor.stop();
					}
					if (greatlinkupAbstractSofa != null) {
						sofaContainer.addChild(greatlinkupAbstractSofa);
					} else {
						abstractProcessor.stop();
					}
					if (greatlinkupWeightedAbstractSofa != null) {
						sofaContainer.addChild(greatlinkupWeightedAbstractSofa);
					} else {
						weightedAbstractProcessor.stop();
					}
					if (greatlinkupWeightedIndexSofa != null) {
						sofaContainer.addChild(greatlinkupWeightedIndexSofa);
					} else {
						weightedIndexProcessor.stop();
					}
				}				
			}
		}
		if (false) {
			try {
				String text = prettySerializer.getXmlAsString(root);
				logger.info(text);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		storeRoot(root);
	}

	private void storeRoot(TagNode root2) {
		try {
			Document doc = storeSerializer.createDOM(root);
			Class cl = Class.forName(existDriverName);
			Database database = (Database) cl.newInstance();
			DatabaseManager.registerDatabase(database);
			Collection coll  = DatabaseManager.getCollection(existURLString
					+ processCollectionPath, existUserName, existUserPassword);
			if (coll != null) {
				XMLResource resource = (XMLResource) coll.createResource(coll.createId(), "XMLResource");
				logger.info("Entry document " + processCollectionPath + "/" + resource.getDocumentId());
				resource.setContentAsDOM(doc);
				coll.storeResource(resource);
				coll.close();
			} else {
				
			}
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (XMLDBException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private TagNode getHtmlPageSofa() {
		if (content == null) {
			return null;
		}

		TagNode htmlNode = null;

		try {
			if (content instanceof File) {
				htmlNode = cleaner.clean((File) content);
			} else if (content instanceof String) {
				htmlNode = cleaner.clean((String) content);
			} else if (content instanceof InputStream) {
				htmlNode = cleaner.clean((InputStream) content);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			return null;
		}

		if (htmlNode != null) {
			bodyText = HTMLCleanerUtil.getBodyText(cleaner, htmlNode, metaHeadersNode);
			bodyText = bodyText.replaceAll("\\t+", " ");
			bodyText = bodyText.replaceAll(" +", " ");
			bodyText = bodyText.replaceAll(" \\n", "\n");
			bodyText = bodyText.replaceAll("\\n+", "\n");

			TagNode sofa = new TagNode(EXIST_SOFA_ELEMENT, cleaner);
			sofa.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_SOFA_NAME, "Text"));
			sofa.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_SOFA_VALUE, bodyText));
			return sofa;
		}
		return null;
	}

	private TagNode addSofaContainerToRoot() {
		TagNode sofaContainer = new TagNode(EXIST_SOFA_CONTAINER, cleaner);
		root.addChild(sofaContainer);
		return sofaContainer;
	}

	private void addSyndFeedToRoot() {
		TagNode feedNode = new TagNode(EXIST_HEADER_ELEMENT, cleaner);
		feedNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_HEADER_TITLE, syndFeed.getTitle()));
		feedNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_HEADER_DESCRIPTION, syndFeed.getDescription()));
		feedNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_HEADER_FEED_TYPE, syndFeed.getFeedType()));
		feedNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_HEADER_LINK, syndFeed.getLink()));
		root.addChild(feedNode);
	}

	private void addSyndEntryToRoot() {
		TagNode entryNode = new TagNode(EXIST_ENTRY_ELEMENT, cleaner);
		entryNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_ENTRY_TITLE, syndEntry.getTitle()));
		entryNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_ENTRY_DESCRIPTION, (syndEntry.getDescription() != null) ? syndEntry.getDescription().getValue() : ""));
		entryNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_ENTRY_AUTHOR, syndEntry.getAuthor()));
		entryNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_ENTRY_LINK, syndEntry.getLink()));
		entryNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_ENTRY_PUBLISHED, QueueUtility.getGMTString(syndEntry.getPublishedDate())));
		entryNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_ENTRY_UPDATED, QueueUtility.getGMTString(syndEntry.getUpdatedDate())));
		entryNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, EXIST_ENTRY_PROCESSED, QueueUtility.getGMTStringForNow()));
		root.addChild(entryNode);

		entryNode.addChild(headersNode);
		metaHeadersNode = new TagNode("MetaHeaders");
		entryNode.addChild(metaHeadersNode);
	}

	private void processLink(String link) {
		try {
			finalURLString = link;
			redirectList.add(link);
			logger.info("EntryProcessor URI[hopCount = " + hopCount + "]: " + finalURLString);
			URL url = new URL(link);
			URLConnection connection = url.openConnection();
			Map map = connection.getHeaderFields();
			Set entrySet = map.entrySet();
			Iterator iterator = entrySet.iterator();
			while (iterator.hasNext()) {
				Map.Entry entry = (Entry) iterator.next();
				String key;
				String value;
				if (entry != null) {
					if (entry.getValue() != null) {
						value = entry.getValue().toString();
					} else {
						value = "null";
					}
					if (entry.getKey() != null) {
						key = entry.getKey().toString();
						if (key.equalsIgnoreCase("Location")) {
							continue;
						} else if (key.equalsIgnoreCase("Content-Type")) {
							if ((value != null) && (value.indexOf("text/html") >= 0)) {
								content = connection.getContent();
							}
						}
					} else {
						key = "HTTP-Status";
						if (value.indexOf("200") < 0) {
							if (value.indexOf("302") >= 0) {
								++hopCount;
								List redirect = (List)map.get("Location");
								String redirectString = redirect.get(0).toString();
								processLink(redirectString);
								return;
							} else {
								System.out.println("Outlier: " + value);
							}
						}
					}
					TagNode httpHeader = new TagNode("HTTPHeader");
					httpHeader.addChild(HTMLCleanerUtil.newTextNode(cleaner, "Key", key));
					httpHeader.addChild(HTMLCleanerUtil.newTextNode(cleaner, "Value", value));
					headersNode.addChild(httpHeader);
				}
			}
			TagNode httpHeader = new TagNode("HTTPHeader");
			httpHeader.addChild(HTMLCleanerUtil.newTextNode(cleaner, "Key", "Location"));
			TagNode locations = new TagNode("Values");
			httpHeader.addChild(locations);
			int index = 0;
			if (redirectList != null) {
				Iterator it = redirectList.iterator();
				while(it.hasNext()) {
					String aLink = (String) it.next();
					TagNode location = new TagNode("Value");
					locations.addChild(location);
					location.addChild(HTMLCleanerUtil.newTextNode(cleaner, "Key", Integer.toString(index++)));
					location.addChild(HTMLCleanerUtil.newTextNode(cleaner, "Value", aLink));
				}
			}
			headersNode.addChild(httpHeader);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


}
