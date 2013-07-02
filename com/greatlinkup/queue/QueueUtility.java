/**
 * 
 */
package com.greatlinkup.queue;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.exist.xmldb.CollectionManagementServiceImpl;
import org.exist.xmldb.XPathQueryServiceImpl;
import org.exist.xmldb.XQueryService;
import org.exist.xupdate.XUpdateProcessor;
import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.base.Resource;
import org.xmldb.api.base.ResourceIterator;
import org.xmldb.api.base.ResourceSet;
import org.xmldb.api.base.Service;
import org.xmldb.api.base.XMLDBException;
import org.xmldb.api.modules.CollectionManagementService;
import org.xmldb.api.modules.XMLResource;
import org.xmldb.api.modules.XUpdateQueryService;

/**
 * @author lcahlander
 * 
 */
public class QueueUtility {

	/** The Constant logger. */
	protected final static Logger logger = Logger.getLogger(QueueUtility.class);

	/**
	 * 
	 */
	public static final String UTC_FORMAT = "yyyy-MM-dd hh:mm:ss";

	/**
	 * 
	 */
	public static final String GMT = "GMT";

	/**
	 * 
	 * @return
	 */
	public static String getGMTStringForNow() {
		Timestamp now = new Timestamp(System.currentTimeMillis());
		return getGMTString(now);
	}

	/**
	 * 
	 * @param date
	 * @return
	 */
	public static String getGMTString(Date date) {
		if (date != null) {
			SimpleDateFormat sdf = new SimpleDateFormat(UTC_FORMAT);
			sdf.setTimeZone(TimeZone.getTimeZone(GMT));
			String string = sdf.format(date);
			string = string.replaceAll(" ", "T");
			return string + "Z";
		}
		return "";
	}

	/**
	 * 
	 * @param existURIString
	 * @param collectionPathString
	 * @param existUserName
	 * @param existUserPassword
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Collection getCollection(String existURIString, String collectionPathString, String existUserName,
			String existUserPassword) {
		String driver = "org.exist.xmldb.DatabaseImpl";
		Collection coll = null;
		try {
			Class cl = Class.forName(driver);
			Database database = (Database) cl.newInstance();
			DatabaseManager.registerDatabase(database);
			String existCollection = null;
			coll = DatabaseManager.getCollection(existURIString + existCollection, existUserName, existUserPassword);
			if (coll == null) {
				int index = collectionPathString.lastIndexOf("/");
				if (index > 0) {
					String collectionPathSubString = collectionPathString.substring(0, index);
					String newCollectionString = collectionPathString.substring(index + 1);
					Collection root = getCollection(existURIString, collectionPathSubString, existUserName, existUserPassword);
					if (root != null) {
						CollectionManagementService mgtService = (CollectionManagementService) root.getService(
								"CollectionManagementService", "1.0");
						coll = mgtService.createCollection(newCollectionString);
					} else {
						return null;
					}
				} else {
					return null;
				}
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (XMLDBException e) {
			e.printStackTrace();
		}
		return coll;
	}

	/**
	 * 
	 * @param existURIString
	 * @param resourcePathString
	 * @param existUserName
	 * @param existUserPassword
	 * @return
	 */
	public static Resource getResource(String existURIString, String resourcePathString, String resourceName, String existUserName,
			String existUserPassword) {
		Resource resource = null;
		Collection collection = getCollection(existURIString, resourcePathString, existUserName, existUserPassword);
		if (collection != null) {
			try {
				resource = collection.getResource(resourceName);
				
				if (resource == null) {
					resource = collection.getResource(resourcePathString.substring(4) + "/" + resourceName);
				}
			} catch (XMLDBException e) {
				e.printStackTrace();
			}
		}
		return resource;
	}

	/**
	 * 
	 * @param resource
	 * @param tagPath
	 * @param service
	 * @return
	 * @throws XMLDBException
	 */
	public static String getTagTextFromResource(Resource resource, String tagPath) throws XMLDBException {
		return getTagTextFromResource(resource, tagPath, null);
	}

	/**
	 * 
	 * @param resource
	 * @param tagPath
	 * @param service
	 * @return
	 * @throws XMLDBException
	 */
	public static String getTagTextFromResource(Resource resource, String tagPath, String defaultValue) throws XMLDBException {
		XPathQueryServiceImpl service = (XPathQueryServiceImpl) resource.getParentCollection().getService("XPathQueryService",
				"1.0");
		ResourceSet set = service.query((XMLResource) resource, tagPath + "/text()");
		XMLResource res1 = (XMLResource) set.getResource(0);
		if (res1 != null)
			return (String) res1.getContent();
		return defaultValue;
	}

	/**
	 * 
	 * @param resource
	 * @param tagPath
	 * @param service
	 * @return
	 * @throws XMLDBException
	 */
	public static void updateDateTimeTagTextToNowFromResourceIfEmpty(Resource resource, String tagPath) throws XMLDBException {
		XPathQueryServiceImpl service = (XPathQueryServiceImpl) resource.getParentCollection().getService("XPathQueryService",
				"1.0");
		ResourceSet set = service.query((XMLResource) resource, "//" + tagPath + "/text()");
		XMLResource res1 = (XMLResource) set.getResource(0);
		if (((res1 != null) && (((String) res1.getContent()).length() <= 0)) || (res1 == null)) {
			String xupdate = "<xu:modifications version=\"1.0\" xmlns:xu=\"" + XUpdateProcessor.XUPDATE_NS + "\">"
					+ "<xu:variable name=\"now\" select=\"current-dateTime()\"/>" + "<xu:update select=\"//" + tagPath
					+ "\"><xu:value-of select=\"$now\"/></xu:update>" + "</xu:modifications>";

			doXUpdateResource(resource, xupdate);
		}
	}

	public static void updateTagTextFromResource(Resource resource, String tagPath, String tagValue) throws XMLDBException {
		String xupdate = "<xu:modifications version=\"1.0\" xmlns:xu=\"" + XUpdateProcessor.XUPDATE_NS + "\">"
				+ "<xu:update select=\"//" + tagPath + "\">" + tagValue + "</xu:update>" + "</xu:modifications>";

		doXUpdateResource(resource, xupdate);
	}

	/**
	 * 
	 * @param feed
	 * @param tagPath
	 * @param text
	 * @return
	 */
	public static boolean setTagTextFromResource(Resource feed, String tagPath, String text) {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * 
	 * @param sourceURLString
	 *            xmldb:exist://localhost:8080/exist/xmlrpc
	 * @param userName
	 *            admin
	 * @param password
	 *            admin123
	 * @param sourceResourcePath
	 *            /db/greatlinkup/apps/job-scheduler/data/queue
	 * @param sourceName
	 *            0000001feed0002.xml
	 * @param targetURLString
	 *            xmldb:exist://localhost:8080/exist/xmlrpc
	 * @param targetResourcePath
	 *            /db/syntacticq/apps/job-scheduler/data/cust/00000001/completed
	 * @return
	 * @throws XMLDBException 
	 */
	public static boolean moveResource(String sourceURLString, String userName, String password, String sourceResourcePath,
			String sourceName, String targetURLString, String targetResourcePath) throws XMLDBException {

		System.out.println("sourceURLString      = [" + sourceURLString + "]");
		System.out.println("sourceResourcePath   = [" + sourceResourcePath + "]");
		System.out.println("sourceName           = [" + sourceName + "]");
		System.out.println("targetURLString      = [" + targetURLString + "]");
		System.out.println("targetResourcePath   = [" + targetResourcePath + "]");
		if (sourceURLString.equals(targetURLString)) {
			Collection sourceCollection = getCollection(sourceURLString, sourceResourcePath, userName, password);
			Resource resource = sourceCollection.getResource(sourceResourcePath.substring(4) + "/" + sourceName);
			if (resource != null) {
				CollectionManagementServiceImpl service = (CollectionManagementServiceImpl) sourceCollection.getService(
						"CollectionManagementService", "1.0");
				service.moveResource(sourceResourcePath.substring(4) + "/" + sourceName, targetResourcePath, null);
				return true;
			}
			// CollectionManagementService
			/*
			 * Move resource within same eXist database.
			 */
		} else {
			/*
			 * Copy resource from source and create in target. Delete resource
			 * from source.
			 */
		}
		return false;
	}

	public static Resource doQuery(Collection collection, String query) throws XMLDBException {
		XQueryService service = (XQueryService) collection.getService("XQueryService", "1.0");
		ResourceSet result = service.query(query);
		logger.info("Found " + result.getSize() + " results.");
		for (ResourceIterator i = result.getIterator(); i.hasMoreResources();) {
			return i.nextResource();
		}
		return null;
	}

	public static void doXUpdate(Collection collection, String xupdate) throws XMLDBException {
		if ((xupdate == null) || (xupdate.length() <= 0)) {
			logger.error("doXUpdate - no update string was passed in.");
			return;
		}
		XUpdateQueryService service = (XUpdateQueryService) collection.getService("XUpdateQueryService", "1.0");
		long mods = service.update(xupdate);
		if (mods == 0) {
			logger.info("no modifications processed for: \n\n" + xupdate);
		} else {
			logger.info(mods + " modifications processed");
		}
	}

	public static void doXUpdateResource(Resource resource, String xupdate) {
		if ((xupdate == null) || (xupdate.length() <= 0)) {
			logger.error("doXUpdate - no update string was passed in.");
			return;
		}
		long mods = 0;
		try {
			Collection collection = resource.getParentCollection();
			// Service[] services = collection.getServices();
			// for (int index = 0; index < services.length; index++) {
			// System.out.println(services[index].getName() + ":" +
			// services[index].getVersion());
			// }
			String collectionPath = collection.getName().substring(4);
			String resourcePath = collectionPath + "/" + resource.getId();
			XUpdateQueryService service = (XUpdateQueryService) collection.getService("XUpdateQueryService", "1.0");
			System.out.println("doXUpdate[" + resourcePath + "][" + xupdate + "]");
			mods = service.updateResource(resourcePath, xupdate);
		} catch (XMLDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info(mods + " modifications processed.");
	}
}
