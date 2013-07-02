/**
 * 
 */
package com.greatlinkup.queue;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.exist.scheduler.JobException;
import org.exist.scheduler.UserJavaJob;
import org.exist.storage.BrokerPool;
import org.exist.storage.txn.TransactionManager;
import org.exist.storage.txn.Txn;
import org.exist.xupdate.XUpdateProcessor;
import org.quartz.SchedulerConfigException;
import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.base.Resource;
import org.xmldb.api.base.XMLDBException;


// TODO: Auto-generated Javadoc
/**
 * In <strong>conf.xml</strong>
 * <p>
 * 
 * &nbsp;&nbsp;&nbsp;&nbsp;&lt;job
 * class="com.greatlinkup.queue.QueueProcessingJavaJob" period="60000"
 * delay="60000" repeat="2" &gt;<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;parameter
 * name="existUserPassword" value=""/&gt;<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;parameter
 * name="existUserName" value="admin"/&gt;<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;parameter
 * name="existCustomerBaseString" value="/db/cust"/&gt;<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;parameter
 * name="existInProcessString" value="/db/inprocess"/&gt;<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;parameter
 * name="existInQueueString" value="/db/queue"/&gt;<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;parameter
 * name="existURLString" value="xmldb:exist://localhost:8080/exist/xmlrpc"/&gt;<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&lt;/job&gt;<br>
 * 
 * @author Loren Cahlander
 */
public class QueueProcessingJavaJob extends UserJavaJob {

	public static final String ORG_EXIST_XMLDB_DATABASE_IMPL = "org.exist.xmldb.DatabaseImpl";

	public static final String PARTS_OF_SPEECH_URL = "partsOfSpeechURL";

	public static final String QUEUE_NODE_ID = "QueueNodeID";

	public static final String QUEUE_NODE_RESOURCE_NAME = "QueueNodeResourceName";

	public static final String PROCESS_RESOURCE_NAME = "ProcessResourceName";

	public static final String PROCESS_COLLECTION_PATH = "ProcessCollectionPath";

	public static final String CUSTOMER_RESOURCE_NAME = "CustomerResourceName";

	public static final String CUSTOMER_COLLECTION_PATH = "CustomerCollectionPath";
	
	public static final String CUSTOMER_COMPLETED_COLLECTION_PATH = "CustomerCompletedCollectionPath";
	
	public static final String CUSTOMER_ERROR_COLLECTION_PATH = "CustomerErrorCollectionPath";
	
	public static final String THREAD_JAVA_CLASS_NAME = "ThreadJavaClassName";

	public static final String CUSTOMER_ID = "CustomerID";

	/** The Constant logger. */
	protected final static Logger logger = Logger
			.getLogger(QueueProcessingJavaJob.class);
	
	public static ThreadPoolExecutor primaryThreadPool = null;
	
	/** The name. */
	private String name = "QueueProcessingJavaJob";
	
	/**
	 * This is the parameter to the Quartz Scheduler Job that identifies the
	 * eXist user that will be processed against.
	 */
	public static final String EXIST_USER_PASSWORD = "existUserPassword";
	
	/**
	 * This is the parameter to the Quartz Scheduler Job that identifies the
	 * eXist user password.
	 */
	public static final String EXIST_USER_NAME = "existUserName";
	
	/**
	 * This is the parameter to the Quartz Scheduler Job that identifies the
	 * base eXist Collection for customers that the job will store the resulting
	 * data in.
	 */
	public static final String EXIST_CUSTOMER_BASE_STRING = "existCustomerBaseString";
	
	/**
	 * This is the parameter to the Quartz Scheduler Job that identifies the
	 * eXist Collection where the unprocessed queue entries resides.
	 */
	public static final String EXIST_IN_QUEUE_STRING = "existInQueueString";
	
	/**
	 * This is the parameter to the Quartz Scheduler Job that identifies the
	 * eXist Collection where the in process queue entries resides.
	 */
	public static final String EXIST_URL_STRING = "existURLString";
	
	public static final String FEED_PROCESS_PRIORITY_STRING = "feedProcessPriority";

	/** The exist url string. */
	protected String existURLString;
	
	protected String feedProcessPriority = "5";
	
	/** The exist in queue string. */
	protected String existInQueueString = "/db/greatlinkup/apps/job-scheduler/data/queue";
	
	/** The exist customer base string. */
	protected String existCustomerBaseString = "/db/greatlinkup/apps/job-scheduler/data/cust";
	
	/** The exist user name. */
	protected String existUserName;
	
	/** The exist user password. */
	protected String existUserPassword;
	
	/** The exist in queue collection. */
	protected Collection existInQueueCollection;
	
	/** The params. */
	protected Map<String,String> params = new HashMap<String,String>();
	
	private static final String XUPDATE_SET_HARVESTING_START_DATETIME = "<xu:modifications version=\"1.0\" xmlns:xu=\"" + XUpdateProcessor.XUPDATE_NS + "\">" +
	"<xu:insert-after select=\"//Customer/CreationDateTime\">" +
	"<HarvestingStartDateTime>now</HarvestingStartDateTime>" +
	"</xu:insert-after>" +
	"</xu:modifications>";

	private static final String XUPDATE_MOVE_TO_ERROR_QUEUE = null;

	private String customerID;

	private String threadJavaClassName;

	private String customerCollectionPath;

	private String customerResourceName;

	private String processCollectionPath;

	private String processResourceName;

	private String queueNodeResourceName;

	private String queueNodeId;

	private String customerCompletedCollectionPath;

	private String customerErrorCollectionPath;

	private String partsOfSpeechURL;

	
	/**
	 * Execute.
	 * 
	 * @param brokerPool
	 *            the broker pool
	 * @param params
	 *            the params
	 * 
	 * @throws JobException
	 *             the job exception
	 * 
	 * @see org.exist.scheduler.UserJavaJob#execute(org.exist.storage.BrokerPool,
	 *      java.util.Map)
	 */
	@SuppressWarnings("unchecked")
	public void execute(BrokerPool brokerPool, Map params) throws JobException {
		logger.info("Entering QueueProcessingJavaJob");
		Set paramsSet = params.entrySet();
		Iterator iterator = paramsSet.iterator();
		while (iterator.hasNext()) {
			Entry entry = (Entry)iterator.next();
			this.params.put((String)entry.getKey(), (String)entry.getValue());
		}
		existURLString = this.params.get(EXIST_URL_STRING);
		existInQueueString = this.params.get(EXIST_IN_QUEUE_STRING);
		existCustomerBaseString = this.params.get(EXIST_CUSTOMER_BASE_STRING);
		existUserName = this.params.get(EXIST_USER_NAME);
		existUserPassword = this.params.get(EXIST_USER_PASSWORD);
		partsOfSpeechURL = this.params.get(PARTS_OF_SPEECH_URL);
		if (this.params.get(FEED_PROCESS_PRIORITY_STRING) != null)
			feedProcessPriority = this.params.get(FEED_PROCESS_PRIORITY_STRING);

		if (logger.isInfoEnabled()) {
			logger.info("existURLString          [" + existURLString + "]");
			logger.info("existUserName           [" + existUserName + "]");
			logger.info("existCustomerBaseString [" + existCustomerBaseString + "]");
			logger.info("existInQueueString      [" + existInQueueString + "]");
			logger.info("feedProcessPriority     [" + feedProcessPriority + "]");
			logger.info("partsOfSpeechURL        [" + partsOfSpeechURL + "]");
		}
		this.params.put(QueueThreadFactory.THREAD_PRIORITY, feedProcessPriority);
		this.params.put(QueueThreadFactory.THREAD_NAME, "FeedProcessor");
		
		try {
			initializeThreadPools(this.params);
		} catch (SchedulerConfigException e1) {
			e1.printStackTrace();
			primaryThreadPool = null;
			return;
		}
		
		if (primaryThreadPool.getMaximumPoolSize() <= (primaryThreadPool.getActiveCount() + 2)) {
			logger.debug("No threads available in primaryThreadPool.");
			return;
		}

        try {
			// initialize database driver
			Class cl = Class.forName(ORG_EXIST_XMLDB_DATABASE_IMPL);
			Database database = (Database) cl.newInstance();
			DatabaseManager.registerDatabase(database);

			// get the collection
			existInQueueCollection = DatabaseManager.getCollection(existURLString + existInQueueString, existUserName, existUserPassword);
			
			if (existInQueueCollection == null) {
				logger.error("Input Queue [" + existInQueueString + "] does not exist.");
				existInQueueCollection = null;
				return;
			}
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InstantiationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (XMLDBException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		for (int index = 0; index < 5; index++) {
			TransactionManager transact = brokerPool.getTransactionManager();
	        Txn transaction = transact.beginTransaction();
			try {
				/*
				 * Grab the first QueueNode from the queue collection.
				 */
				Resource queueNodeResource = QueueUtility.doQuery(existInQueueCollection, "xmldb:document()//QueueNode[(string-length(StartedDateTime/text()) = 0)]");
				
				if (queueNodeResource == null) {
					/*
					 * If the queue is empty, then end this job process.
					 */
					logger.info("Input Queue [" + existInQueueString + "] is empty.");
					existInQueueCollection = null;
		            transact.abort(transaction);
					return;
				}
				
				/*
				 * Extract the tag values from the QueueNode.
				 */
				queueNodeResourceName = queueNodeResource.getId();
				
				queueNodeId = QueueUtility.getTagTextFromResource(queueNodeResource, QUEUE_NODE_ID);
				
				customerID = QueueUtility.getTagTextFromResource(queueNodeResource, CUSTOMER_ID);
		        threadJavaClassName = QueueUtility.getTagTextFromResource(queueNodeResource, THREAD_JAVA_CLASS_NAME);
		        customerCollectionPath = QueueUtility.getTagTextFromResource(queueNodeResource, CUSTOMER_COLLECTION_PATH);
				customerResourceName = QueueUtility.getTagTextFromResource(queueNodeResource, CUSTOMER_RESOURCE_NAME);
		        processCollectionPath = QueueUtility.getTagTextFromResource(queueNodeResource, PROCESS_COLLECTION_PATH);
				processResourceName = QueueUtility.getTagTextFromResource(queueNodeResource, PROCESS_RESOURCE_NAME);
		        customerCompletedCollectionPath = QueueUtility.getTagTextFromResource(queueNodeResource, CUSTOMER_COMPLETED_COLLECTION_PATH);
		        customerErrorCollectionPath = QueueUtility.getTagTextFromResource(queueNodeResource, CUSTOMER_ERROR_COLLECTION_PATH);

		        /*
		         * Mark the QueueNode as in process by setting the StartedDateTime tag to now.
		         */
				QueueUtility.updateDateTimeTagTextToNowFromResourceIfEmpty(queueNodeResource, "StartedDateTime");
				transact.commit(transaction);
			} catch (Exception e) {
			}
			
			try {
				if (logger.isInfoEnabled()) {
					logger.info("existURLString          [" + existURLString + "]");
					logger.info("customerCollectionPath  [" + customerCollectionPath + "]");
					logger.info("customerResourceName    [" + customerResourceName + "]");
				}
				/*
				 * Retrieve the collection for the customer specified by {QueueNode.CustomerCollectionPath} from this eXist server.
				 */
				Collection existCustomerCollection = DatabaseManager.getCollection(existURLString + customerCollectionPath, existUserName, existUserPassword);

				if (existCustomerCollection == null) {
					/*
					 * The customer collection does not exist.
					 * 
					 * Log it!
					 */
					logger.error("Customer Collection [" + customerCollectionPath + "] does not exist.");
					/*
					 * TODO: Move the QueueNode to the errorQueue.
					 */
					QueueUtility.doXUpdate(existInQueueCollection, XUPDATE_MOVE_TO_ERROR_QUEUE);
				} else {
					Resource customerRecordResource = existCustomerCollection.getResource(customerResourceName);

					if (customerRecordResource == null) {
						/*
						 * The {Customer} resource does not exist.
						 * 
						 * Log the error.
						 * 
						 */
						logger.error("Customer resource [" + customerCollectionPath + "/" + customerResourceName + "] does not exist.");
						/*
						 * TODO: Move the QueueNode to the errorQueue.
						 */
						QueueUtility.doXUpdate(existInQueueCollection, XUPDATE_MOVE_TO_ERROR_QUEUE);
					} else {
						/*
						 * If this is the first record for harvesting for the customer, then mark the start of the harvesting in the customer record.
						 */
						QueueUtility.updateDateTimeTagTextToNowFromResourceIfEmpty(customerRecordResource, "HarvestingStartDateTime");
						
						/*
						 * Create a Map with all of the parameters to pass to the {QueueNode.ThreadJavaClassName} Runnable class.
						 */
						Map<String,String> feedMap = new HashMap<String,String>(params);
						feedMap.put(CUSTOMER_ID, customerID);
						feedMap.put(PROCESS_COLLECTION_PATH, processCollectionPath);
						feedMap.put(PROCESS_RESOURCE_NAME, processResourceName);
						feedMap.put(CUSTOMER_COMPLETED_COLLECTION_PATH, customerCompletedCollectionPath);
						feedMap.put(CUSTOMER_ERROR_COLLECTION_PATH, customerErrorCollectionPath);
						feedMap.put(CUSTOMER_COLLECTION_PATH, customerCollectionPath);
						feedMap.put(CUSTOMER_RESOURCE_NAME, customerResourceName);
						feedMap.put(EXIST_URL_STRING, existURLString);
						feedMap.put(EXIST_IN_QUEUE_STRING, existInQueueString);
						feedMap.put(QUEUE_NODE_RESOURCE_NAME, queueNodeResourceName);
						feedMap.put(QUEUE_NODE_ID, queueNodeId);
						feedMap.put(EXIST_CUSTOMER_BASE_STRING, existCustomerBaseString);
						feedMap.put(EXIST_USER_NAME, existUserName);
						feedMap.put(EXIST_USER_PASSWORD, existUserPassword);
						feedMap.put(PARTS_OF_SPEECH_URL, partsOfSpeechURL);
						
						logger.trace("Spawning thread for " + threadJavaClassName);
						
						Class<?> runnableClass = Class.forName(threadJavaClassName);
						Constructor constructor =
							runnableClass.getConstructor(new Class[]{Map.class});
						Runnable runnable = (Runnable) constructor.newInstance(feedMap);

						primaryThreadPool.execute(runnable);
					}
				}
				existInQueueCollection = null;
				logger.debug("Transaction committed.");
			} catch (Exception e) {
				logger.error("Failed to process first document in " + existInQueueString, e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void initializeThreadPools(Map<String,String> params2) throws SchedulerConfigException {
		// TODO Auto-generated method stub
		if (primaryThreadPool == null) {
			BlockingQueue q = new ArrayBlockingQueue(20);
			ThreadFactory tf = new QueueThreadFactory(params2);
			ThreadPoolExecutor ex = new ThreadPoolExecutor(4, 10, 20, TimeUnit.SECONDS, q, tf);
			primaryThreadPool = ex;
		}
	}

	/**
	 * Gets the name.
	 * 
	 * @return the name
	 * 
	 * @see org.exist.scheduler.JobDescription#getName()
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the name.
	 * 
	 * @param name
	 *            the name
	 * 
	 * @see org.exist.scheduler.JobDescription#setName(java.lang.String)
	 */
	public void setName(String name) {
		this.name = name;
	}

}
