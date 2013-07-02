/*
 * Copyright (c) 2009 greatlinkup, Inc. All Rights Reserved.
 *
 * greatlinkup grants you ("Licensee") a non-exclusive, royalty free, license to use,
 * modify and redistribute this software in source and binary code form,
 * provided that i) this copyright notice and license appear on all copies of
 * the software; and ii) Licensee does not utilize the software in a manner
 * which is disparaging to Sun.
 *
 * This software is provided "AS IS," without a warranty of any kind. ALL
 * EXPRESS OR IMPLIED CONDITIONS, REPRESENTATIONS AND WARRANTIES, INCLUDING ANY
 * IMPLIED WARRANTY OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE OR
 * NON-INFRINGEMENT, ARE HEREBY EXCLUDED. SUN AND ITS LICENSORS SHALL NOT BE
 * LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING
 * OR DISTRIBUTING THE SOFTWARE OR ITS DERIVATIVES. IN NO EVENT WILL SUN OR ITS
 * LICENSORS BE LIABLE FOR ANY LOST REVENUE, PROFIT OR DATA, OR FOR DIRECT,
 * INDIRECT, SPECIAL, CONSEQUENTIAL, INCIDENTAL OR PUNITIVE DAMAGES, HOWEVER
 * CAUSED AND REGARDLESS OF THE THEORY OF LIABILITY, ARISING OUT OF THE USE OF
 * OR INABILITY TO USE SOFTWARE, EVEN IF SUN HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGES.
 *
 * This software is not designed or intended for use in on-line control of
 * aircraft, air traffic, aircraft navigation or aircraft communications; or in
 * the design, construction, operation or maintenance of any nuclear
 * facility. Licensee represents and warrants that it will not use or
 * redistribute the Software for such purposes.
 */
package com.greatlinkup.queue;

import java.util.Map;
import java.util.concurrent.ThreadFactory;

/**
 * @author lcahlander
 *
 */
public class QueueThreadFactory implements ThreadFactory {
	
	public static final String THREAD_NAME = "threadName";
	public static final String THREAD_PRIORITY = "threadPriority";
	public int threadPriority = 3;
	public String threadName = "greatlinkupQueue";
	private int index = 0;
	
	public QueueThreadFactory(Map<String, String> parameters) {
		if (parameters != null) {
			String value = parameters.get(THREAD_PRIORITY);
			if (value != null) {
				int intValue = Integer.parseInt(value);
				if (intValue >= Thread.MIN_PRIORITY && intValue <= Thread.MAX_PRIORITY) {
					threadPriority = intValue;
				}
			}
			value = parameters.get(THREAD_NAME);
			if (value != null && value.length() > 0) {
				threadName = value;
			}
		}
		
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
	 */
	public Thread newThread(Runnable r) {
		++index;
	     Thread thread =  new Thread(null, r, threadName + "-" + index);
	     thread.setPriority(threadPriority);
	     return thread;
	     }

}
