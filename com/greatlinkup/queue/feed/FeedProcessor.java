/*
 * 
 */
package com.greatlinkup.queue.feed;

import java.io.IOException;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;
import org.htmlcleaner.CleanerProperties;
import org.htmlcleaner.DomSerializer;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.w3c.dom.Document;
import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.base.Resource;
import org.xmldb.api.base.XMLDBException;
import org.xmldb.api.modules.CollectionManagementService;
import org.xmldb.api.modules.XMLResource;

import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;
import com.greatlinkup.queue.QueueProcessingJavaJob;
import com.greatlinkup.queue.QueueThreadFactory;
import com.greatlinkup.queue.QueueUtility;
import com.greatlinkup.queue.entry.EntryProcessor;
import com.greatlinkup.queue.entry.HTMLCleanerUtil;

/**
 * @author lcahlander
 * 
 */
public class FeedProcessor implements Runnable {

	public static final String ENTRY_THREAD_PRIORITY = "EntryThreadPriority";

	public static final String FEED_THREAD_PRIORITY = "FeedThreadPriority";

	public static final String HARVESTED_DATA_STORAGE_URI = "HarvestedDataStorageURI";

	public static final String HTTP_STATUS_CODE = "HTTPStatusCode";

	public static final String FEED_PROCESSING_START_DATE_TIME = "FeedProcessingStartDateTime";

	public static final String FEED_PROCESSING_COMPLETED_DATE_TIME = "FeedProcessingCompletedDateTime";

	public static final String FEED_URL = "FeedURL";

	public static final String FEED_ID = "FeedID";

	/** The Constant logger. */
	protected final static Logger logger = Logger.getLogger(FeedProcessor.class);

	private String rssFeed = null;
	private String existCollection = null;
	private String existURLString = null;
	private String existUserName;
	private String existUserPassword;
	private String customerCollectionPath = null;
	private String customerID;
	private String existCustomerBaseString;
	private String existInQueueString;
	private String customerResourceName = null;
	private String processCollectionPath = null;
	private String processResourceName = null;
	private String queueNodeResourceName = null;

	private String feedId;

	private Resource feed;

	private String httpStatus;

	private String entryThreadPriority;

	private String queueNodeID;

	private String customerCompletedCollectionPath;

	private String customerErrorCollectionPath;

	private String partsOfSpeechURL;

	/**
	 * 
	 * @param feedMap
	 */
	public FeedProcessor(Map<String, String> feedMap) {
		logger.info("Constructor - Begin");
		processCollectionPath = feedMap.get(QueueProcessingJavaJob.PROCESS_COLLECTION_PATH);
		processResourceName = feedMap.get(QueueProcessingJavaJob.PROCESS_RESOURCE_NAME);
		customerCompletedCollectionPath = feedMap.get(QueueProcessingJavaJob.CUSTOMER_COMPLETED_COLLECTION_PATH);
		customerErrorCollectionPath = feedMap.get(QueueProcessingJavaJob.CUSTOMER_ERROR_COLLECTION_PATH);
		customerCollectionPath = feedMap.get(QueueProcessingJavaJob.CUSTOMER_COLLECTION_PATH);
		customerResourceName = feedMap.get(QueueProcessingJavaJob.CUSTOMER_RESOURCE_NAME);
		customerID = feedMap.get(QueueProcessingJavaJob.CUSTOMER_ID);
		existURLString = feedMap.get(QueueProcessingJavaJob.EXIST_URL_STRING);
		existInQueueString = feedMap.get(QueueProcessingJavaJob.EXIST_IN_QUEUE_STRING);
		existCustomerBaseString = feedMap.get(QueueProcessingJavaJob.EXIST_CUSTOMER_BASE_STRING);
		existUserName = feedMap.get(QueueProcessingJavaJob.EXIST_USER_NAME);
		existUserPassword = feedMap.get(QueueProcessingJavaJob.EXIST_USER_PASSWORD);
		queueNodeResourceName = feedMap.get(QueueProcessingJavaJob.QUEUE_NODE_RESOURCE_NAME);
		queueNodeID = feedMap.get(QueueProcessingJavaJob.QUEUE_NODE_ID);
		partsOfSpeechURL = feedMap.get(QueueProcessingJavaJob.PARTS_OF_SPEECH_URL);

		if (logger.isInfoEnabled()) {
			logger.info("customerID                       [" + customerID + "]");
			logger.info("customerCompletedCollectionPath  [" + customerCompletedCollectionPath + "]");
			logger.info("customerErrorCollectionPath      [" + customerErrorCollectionPath + "]");
			logger.info("customerCollectionPath           [" + customerCollectionPath + "]");
			logger.info("customerResourceName             [" + customerResourceName + "]");
			logger.info("processCollectionPath            [" + processCollectionPath + "]");
			logger.info("processResourceName              [" + processResourceName + "]");
			logger.info("existURLString                   [" + existURLString + "]");
			logger.info("existUserName                    [" + existUserName + "]");
			logger.info("existCustomerBaseString          [" + existCustomerBaseString + "]");
			logger.info("existInQueueString               [" + existInQueueString + "]");
			logger.info("queueNodeResourceName            [" + queueNodeResourceName + "]");
			logger.info("queueNodeID                      [" + queueNodeID + "]");
			logger.info("partsOfSpeechURL                 [" + partsOfSpeechURL + "]");
		}
		logger.info("Constructor - End");
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	public void run() {
		logger.info("run() - Begin");

		if (customerCollectionPath == null) {
			logger.error("No " + QueueProcessingJavaJob.CUSTOMER_COLLECTION_PATH
					+ " parameter in the parameter map.  FeedProcess terminated.");
			return;
		}
		String harvestedDataStorageURLString = existURLString;
		/*
		 * Get the Customer resource.
		 */
		Collection customerCollection = QueueUtility.getCollection(existURLString, customerCollectionPath, existUserName,
				existUserPassword);
		Resource customer = null;
		try {
			customer = QueueUtility.doQuery(customerCollection, "xmldb:document()//Customer");
			// customer = customerCollection.getResource(customerResourceName);
		} catch (XMLDBException e2) {
			logger.error("Error getting customer document.", e2);
		}
		// Resource customer = QueueUtility.getResource(existURLString,
		// customerResourcePathString, existUserName, existUserPassword);
		if (customer == null) {
			logger
					.error("No " + customerCollectionPath + "/" + customerResourceName
							+ " resource exists.  FeedProcess terminated.");
			return;
		} else {
			logger.info("Customer record exists");
		}
		/*
		 * Get the HarvestedDataStorageURI from the Customer resource.
		 */
		try {
			String text = QueueUtility.getTagTextFromResource(customer, HARVESTED_DATA_STORAGE_URI);
			if ((text != null) && (text.length() > 0))
				harvestedDataStorageURLString = text;
		} catch (XMLDBException e1) {
			logger.error("Error getting harvestedDataStorageURLString from customer document.", e1);
		}
		/*
		 * Get the Feed collection.
		 */
		Collection harvestingCollection = QueueUtility.getCollection(harvestedDataStorageURLString, processCollectionPath,
				existUserName, existUserPassword);
		SyndFeed inFeed = null;
		ListIterator li = null;
		List rssList = null;
		try {
			/*
			 * Get the Feed resource.
			 */
			String query = "xmldb:document()//Feed[CustomerID = '" + customerID + "']";
			feed = QueueUtility.doQuery(harvestingCollection, query);
//			String feedResourceFullPath = processCollectionPath.substring(4) + "/" + processResourceName;
//			logger.info("feedResourceFullPath [" + feedResourceFullPath + "]");
//			 feed = harvestingCollection.getResource(feedResourceFullPath);
			if (feed == null) {
				logger.error("No " + "feed"
						+ " resource exists.  FeedProcess terminated.");
				return;
			}
			feedId = QueueUtility.getTagTextFromResource(feed, FEED_ID);
			logger.info("feedId = " + feedId);
			/*
			 * Get the URL for the RSS/Atom feed from the Feed resource.
			 */
			rssFeed = QueueUtility.getTagTextFromResource(feed, FEED_URL);
			logger.info("rssFeed = " + rssFeed);
			entryThreadPriority = QueueUtility.getTagTextFromResource(feed, ENTRY_THREAD_PRIORITY);
			logger.info("entryThreadPriority = " + entryThreadPriority);
			URL feedURL = new URL(rssFeed);
			QueueUtility.updateDateTimeTagTextToNowFromResourceIfEmpty(feed, FEED_PROCESSING_START_DATE_TIME);
			updateHTTPStatusCode(feedURL);
			SyndFeedInput input = new SyndFeedInput();
			XmlReader reader = new XmlReader(feedURL);
			inFeed = input.build((Reader) reader);
			rssList = inFeed.getEntries();
			li = rssList.listIterator();
		} catch (Exception e) {
			logger.error(feedId + ":" + rssFeed + " :: ERROR ========================================");
			badFeedReport("ERROR");
			return;
		}

		if (rssList.isEmpty()) {
			logger.info(feedId + ":" + rssFeed + " :: EMPTY ========================================");
			badFeedReport("EMPTY");
			try {
				QueueUtility.updateDateTimeTagTextToNowFromResourceIfEmpty(feed, FEED_PROCESSING_COMPLETED_DATE_TIME);
			} catch (XMLDBException e) {
				logger.error("Error updating " + FEED_PROCESSING_COMPLETED_DATE_TIME, e);
			}
			return;
		}

		Map<String, String> threadMap = new HashMap<String, String>();
		threadMap.put(QueueThreadFactory.THREAD_PRIORITY, entryThreadPriority);
		threadMap.put(QueueThreadFactory.THREAD_NAME, "EntryProcessor-" + customerID + "-" + feedId);

		ThreadFactory tf = new QueueThreadFactory(threadMap);
		ExecutorService exec = Executors.newFixedThreadPool(4, tf);

		if (true) {
			while (li.hasNext()) {
				SyndEntry syndEntry = (SyndEntry) li.next();

				Map<String, String> parameters = new HashMap<String, String>();
				parameters.put(QueueProcessingJavaJob.CUSTOMER_ID, customerID);
				parameters.put(QueueProcessingJavaJob.PROCESS_COLLECTION_PATH, processCollectionPath);
				parameters.put(QueueProcessingJavaJob.PROCESS_RESOURCE_NAME, processResourceName);
				parameters.put(QueueProcessingJavaJob.CUSTOMER_COLLECTION_PATH, customerCollectionPath);
				parameters.put(QueueProcessingJavaJob.CUSTOMER_RESOURCE_NAME, customerResourceName);
				parameters.put(QueueProcessingJavaJob.EXIST_URL_STRING, existURLString);
				parameters.put(QueueProcessingJavaJob.EXIST_IN_QUEUE_STRING, existInQueueString);
				parameters.put(QueueProcessingJavaJob.EXIST_CUSTOMER_BASE_STRING, existCustomerBaseString);
				parameters.put(QueueProcessingJavaJob.EXIST_USER_NAME, existUserName);
				parameters.put(QueueProcessingJavaJob.EXIST_USER_PASSWORD, existUserPassword);
				parameters.put(QueueProcessingJavaJob.PARTS_OF_SPEECH_URL, partsOfSpeechURL);

				EntryProcessor entryProcessor = new EntryProcessor(inFeed, syndEntry, parameters);
				exec.execute(entryProcessor);
			}
		}

		exec.shutdown();
		while (!exec.isTerminated()) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				logger.error("General error", e);
			}
		}
		try {
			cleanupFeedProcess();
		} catch (XMLDBException e) {
			logger.error("General error", e);
		}
		logger.info("run() - End");
	}

	private void updateHTTPStatusCode(URL feedURL) {
		try {
			URLConnection connection = feedURL.openConnection();
			Map<String, List<String>> headerFieldsMap = connection.getHeaderFields();
			Set entrySet = headerFieldsMap.entrySet();
			Iterator iterator = entrySet.iterator();
			while (iterator.hasNext()) {
				Map.Entry entry = (Entry) iterator.next();
				if (entry.getKey() == null) {
					List<String> valueList = (List<String>) entry.getValue();
					httpStatus = valueList.get(0);
					// httpStatus = (String) entry.getValue();
				}
			}
			QueueUtility.setTagTextFromResource(feed, HTTP_STATUS_CODE, httpStatus);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @throws XMLDBException
	 * 
	 */
	private void cleanupFeedProcess() throws XMLDBException {
		logger.info("Feed resource [" + processCollectionPath + "/" + feed.getId() + "] completed.");
		logger.info("Move [" + existInQueueString + "/" + queueNodeResourceName + "] to [" + customerCollectionPath + "/completed].");
		
		Collection queueCollection = QueueUtility.getCollection(existURLString, existInQueueString, existUserName, existUserPassword);
		Resource queueNode = QueueUtility.doQuery(queueCollection, "xmldb:document()//QueueNode[QueueNodeID/text() = '"+ queueNodeID + "']");
		if (queueNode != null) {
			QueueUtility.updateDateTimeTagTextToNowFromResourceIfEmpty(queueNode, "CompletedDateTime");
		}
		QueueUtility.updateDateTimeTagTextToNowFromResourceIfEmpty(feed, FEED_PROCESSING_COMPLETED_DATE_TIME);

		/*
		 * Move the QueueNode for this Feed to
		 */
		/*
		 * TODO move the queue entry from the 'inProcess' resource to the
		 * 'completed' resource. If , then send out the email message alerting
		 * the customer that they can view the data.
		 */
		QueueUtility.moveResource(existURLString, existUserName, existUserPassword, existInQueueString, queueNodeResourceName, existURLString, customerCompletedCollectionPath);
		/*
		 * TODO: Check to see if there are no more entries for the customer in
		 * either the 'inQueue' or 'inProcess' collections.
		 */
		Resource queueNodeResource = QueueUtility.doQuery(queueCollection, "xmldb:document()//QueueNode[CustomerID/text() = '"+ customerID + "']");
		if (queueNodeResource == null) {
			/*
			 * TODO: If last QueueNode for the Customer, then mark
			 * {Customer.HarvestingCompletedDateTime} to now.
			 */

			/*
			 * TODO: If last QueueNode for the Customer, then mail out the notice
			 * that all of the entries for a customer has been processed. Send a
			 * link to the resource to be viewed.
			 */
		}

	}

	/**
	 * 
	 * @param message
	 */
	@SuppressWarnings("unchecked")
	private void badFeedReport(String message) {
		String driver = "org.exist.xmldb.DatabaseImpl";
		try {
			Class cl = Class.forName(driver);
			Database database = (Database) cl.newInstance();
			DatabaseManager.registerDatabase(database);
			Collection coll = DatabaseManager.getCollection(existURLString + existCollection, existUserName, existUserPassword);
			if (coll == null) {
				// collection does not exist: get root collection and create
				// for simplicity, we assume that the new collection is a
				// direct child of the root collection, e.g. /db/test.
				// the example will fail otherwise.
				Collection root = DatabaseManager.getCollection(existURLString + "/db", existUserName, existUserPassword);
				CollectionManagementService mgtService = (CollectionManagementService) root.getService(
						"CollectionManagementService", "1.0");
				coll = mgtService.createCollection(existCollection.substring("/db".length()));
			}
			HtmlCleaner cleaner = new HtmlCleaner();
			CleanerProperties props = cleaner.getProperties();
			props.setNamespacesAware(false);
			props.setAdvancedXmlEscape(false);
			DomSerializer serializer = new DomSerializer(props);
			TagNode root = new TagNode(EntryProcessor.EXIST_DOCUMENT_ROOT, cleaner);
			TagNode feedNode = new TagNode(EntryProcessor.EXIST_HEADER_ELEMENT, cleaner);
			feedNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, EntryProcessor.EXIST_HEADER_LINK, rssFeed));
			feedNode.addChild(HTMLCleanerUtil.newTextNode(cleaner, "Message", message));
			root.addChild(feedNode);
			Document doc = serializer.createDOM(root);
			XMLResource resource = (XMLResource) coll.createResource(message + ".xml", "XMLResource");
			resource.setContentAsDOM(doc);
			coll.storeResource(resource);
			resource = null;
			cleaner = null;
			serializer = null;
			doc = null;
			coll.close();
			coll = null;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
