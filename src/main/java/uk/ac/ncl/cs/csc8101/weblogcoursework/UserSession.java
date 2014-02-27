package uk.ac.ncl.cs.csc8101.weblogcoursework;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * UserSession class for identifying user sessions and activities in them.
 * The class contain methods to implement a schema for sessions and to query 
 * the session table.
 * @author Saleh Mohamed (103543457)
 * @version 18th February 2014
 */
public class UserSession {
	
	private static Cluster cluster;
	private static Session session;
	//private static final File dataDir = new File("D:/temp/");
	private static final File dataDir = new File("../../");
	private static final File logFile = new File(dataDir, "loglite");
	private static InputStreamReader inputStreamReader;
	private static BufferedReader bufferedReader;
	private static DateFormat dateFormat;
	
	
	/**
	 * Public constructor for creating UserSession object
	 */
	public UserSession() {
		cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build();

		dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]");		

		final int numberOfConnections = 1;
		PoolingOptions poolingOptions = cluster.getConfiguration()
				.getPoolingOptions();
		poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL,
				numberOfConnections);
		poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL,
				numberOfConnections);
		poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE,
				numberOfConnections);
		poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE,
				numberOfConnections);

		final Session bootstrapSession = cluster.connect();
		bootstrapSession
				.execute("CREATE KEYSPACE IF NOT EXISTS coursework WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
		bootstrapSession.shutdown();

		session = cluster.connect("coursework");

		session.execute("CREATE TABLE IF NOT EXISTS user_session (clientId text, sessionId text, startTime bigint, endTime bigint, hitCount bigint, noOfUrls bigint, PRIMARY KEY (clientId, sessionId));");
		FileInputStream fileInputStream;
		try {
			fileInputStream = new FileInputStream(logFile);
			inputStreamReader = new InputStreamReader(fileInputStream);
			bufferedReader = new BufferedReader(inputStreamReader);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	/**
	 * Method that implement a schema for user sessions and create table to store sessions
	 * information.
	 * 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void writeSession() throws InterruptedException, IOException
	{
		int count = 0;   // for testing purposes
		final int maxOutstandingFutures = 4;
		final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(
				maxOutstandingFutures);
		try 
		{	
			// prepared statement for inserting records into the table
			final PreparedStatement insertPS = session.prepare("INSERT INTO user_session (clientId, sessionId, startTime, endTime, hitCount, noOfUrls) "
									+ "VALUES (?, ?, ?, ?, ?, ?)");			
			
			// AtomicReference for storing expired sessions
			final AtomicReference<SiteSession> expiredSession = new AtomicReference<>(null);
			
			// LinkedHashmap to store currently running sessions. Implements the 'removeEldestEntry'
			// to remove the expired sessions from the map.
		    HashMap<String,SiteSession> sessions = new LinkedHashMap<String,SiteSession>(16, 0.75f, true) {
	            protected boolean removeEldestEntry(Map.Entry eldest) {
	                SiteSession siteSession = (SiteSession)eldest.getValue();
	                boolean shouldExpire = siteSession.isExpired();
	                if(shouldExpire) {
	                    expiredSession.set(siteSession);
	                }
	                return siteSession.isExpired();
	            }
	        };
	        
	        String line = bufferedReader.readLine();	// read first line from the log file
	        
	        // stop the loop after reading the last line from the log file
			while (line != null) 
			{	
				int itemsPerBatch = 0;
		        final BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);	
		        
		        // stop this loop when the batch is full or log file is exhausted
		        while (itemsPerBatch < 100 && line != null) 
				{	
		        	// extract details from the line
					String[] tokens = line.split(" ");	
					String id = tokens[0];
					Date timeStamp = dateFormat.parse(tokens[1] + " " + tokens[2]);
					String url = tokens[4];	
					Random rdm = new Random();
					String sessionId = id + "-" + rdm.nextLong() + "";
					
					// create a session object from the new line information
			        SiteSession siteSession = new SiteSession(sessionId, timeStamp.getTime(), url);
			        
			        // check if the map currently contains a session for this user. If it has, check if the 
			        // session has not expired yet, update the session remove the expired session and create 
			        // insert a new session for this user.
			        SiteSession storedSession = sessions.get(id);
			        if (storedSession != null)
			        {
			        	boolean updated = storedSession.isExpired();
			        	if (updated)
			        	{		
			        		sessions.remove(id); 
			        		sessions.put(id, siteSession);
			        	}
			        	else
			        	{
			        		storedSession.update(timeStamp.getTime(), url);
			        	}
			        }
			        else
			        {
			        	sessions.put(id, siteSession);
			        }
					
			        // Check if there is an expired session and store it into the DB
					SiteSession sessionToStore = expiredSession.get();
					if (sessionToStore != null)
					{			
						String sessId = sessionToStore.getId();
						long startTime = sessionToStore.getFirstHitMillis();
						long endTime = sessionToStore.getLastHitMillis();
						long hits = sessionToStore.getHitCount();
						long noUrl = sessionToStore.getHyperLogLog().cardinality();
						String userId = sessId.split("-")[0];																		 
						batchStatement.add(new BoundStatement(insertPS).bind(userId, sessId, startTime, endTime, hits, noUrl));
						itemsPerBatch++;						
																
					}
					System.out.println(++count);	
					
					line = bufferedReader.readLine();	// read next line form the log file
				}
		        
		        outstandingFutures.put(session.executeAsync(batchStatement));
				if (outstandingFutures.remainingCapacity() == 0) 
				{
					ResultSetFuture resultSetFuture = outstandingFutures.take();
					resultSetFuture.getUninterruptibly();
				}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		finally
		{
			bufferedReader.close();
			session.shutdown();
		}

		while (!outstandingFutures.isEmpty()) {
			ResultSetFuture resultSetFuture = outstandingFutures.take();
			resultSetFuture.getUninterruptibly();
		}
	}
	
	/**
	 * Method that takes client id and query the DB to obtain information about
	 * different session for this client
	 * @throws IOException 
	 */
	public void queryUserSessions(String clientId) throws IOException {
		try {
			// prepared statement
			final PreparedStatement selectPS = session
					.prepare("SELECT * FROM user_session WHERE clientId=?;");
			final ResultSetFuture queryFuture = session
					.executeAsync(new BoundStatement(selectPS).bind(clientId));
			
			// get the results and print them on the console
			ResultSet resultSet = queryFuture.getUninterruptibly();
			for (Row row : resultSet) {
				System.out.println(row.getString(0) + " " + row.getString(1)
						+ " " + row.getLong(2) + " " + row.getLong(3) + " "
						+ row.getLong(4) + " " + row.getLong(5));
			}
		} 
		finally 
		{
			bufferedReader.close();
			session.shutdown();
		}
	}
}
