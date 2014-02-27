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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;

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
 * This class creates a schema that can be used to query total number of
 * access for each url given a set of urls, start and end hours.
 * @author Saleh Mohamed (103543457)
 * @version 18th February 2014
 */
public class TotalNumberOfAccesses {
	
	private static Cluster cluster;
    private static Session session;
    //private static final File dataDir = new File("D:/temp/");
    private static final File dataDir = new File("/home/ubuntu/data/cassandra-test-dataset");
	private static final File logFile = new File(dataDir, "CSC8101-logfile.gz");
	private static InputStreamReader inputStreamReader;
    private static BufferedReader bufferedReader;
    private static  DateFormat dateFormat;
    
    /**
	 * Public constructor for creating instances of this class
	 */

    public TotalNumberOfAccesses()
    {
    	 cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build(); 
    	 dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH]");
	
		 final int numberOfConnections = 1;
		
		 PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();
		 poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, numberOfConnections);
		 poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, numberOfConnections);
		 poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, numberOfConnections);
		 poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, numberOfConnections);
		
		 final Session bootstrapSession = cluster.connect();
		 bootstrapSession.execute("CREATE KEYSPACE IF NOT EXISTS coursework WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
		 bootstrapSession.shutdown();
		
		 session = cluster.connect("coursework");
		
		 session.execute("CREATE TABLE IF NOT EXISTS url_access (url text, hour bigint, hits counter, PRIMARY KEY ((url), hour));");
		
		 FileInputStream fileInputStream;
		try {
				fileInputStream = new FileInputStream(logFile);
				final GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
				inputStreamReader = new InputStreamReader(gzipInputStream);
				bufferedReader = new BufferedReader(inputStreamReader);
		} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 
    }
    
    /**
	 * This method reads a line from a log file and extracts user id, timestamp and url from
	 * each line. The information is then stored into the DB
	 * 
	 * @throws InterruptedException, ParseException, IOException
     * @throws IOException 
	 */
    public void writeToDB() throws InterruptedException, IOException
	{
		final int maxOutstandingFutures = 4;
		final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(
				maxOutstandingFutures);
		try 
		{	
			// prepared statement for inserting records into the table
			final PreparedStatement updatePS = session
					.prepare("UPDATE url_access SET hits = hits + ? WHERE url = ? AND hour = ?;");
			int count = 0;
			String line = bufferedReader.readLine();	// read first line from the log file
			
			// stop the loop after reading the last line from the log file
			while (line != null) 
			{					
				int itemsPerBatch = 0;
				final BatchStatement batchStatement = new BatchStatement(
						BatchStatement.Type.COUNTER);
				
				 // stop this loop when the batch is full
				while (itemsPerBatch < 100) 
				{						
					if (line == null)
						break;
					
					// read a line and extract user id, timestamp and and url from it. 
					String[] tokens = line.split(" ");	
					String date = tokens[1].substring(0, 15) + "]";
					Date timeStamp = (Date) dateFormat.parse(date);
					String url = tokens[4];										 
					batchStatement.add(new BoundStatement(updatePS).bind(1L, url, timeStamp.getTime()));
					itemsPerBatch++;
					line = bufferedReader.readLine();	 // read next line
					System.out.println(++count);
				}
				// when the batch is full, execute asynchronously
				outstandingFutures.put(session.executeAsync(batchStatement));				
				if (outstandingFutures.remainingCapacity() == 0) {
					ResultSetFuture resultSetFuture = outstandingFutures.take();
					resultSetFuture.getUninterruptibly();
				}
			}
			while (!outstandingFutures.isEmpty()) {
				ResultSetFuture resultSetFuture = outstandingFutures.take();
				resultSetFuture.getUninterruptibly();
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

		
		
	}
    
    /**
	 *  This method sends a query to a DB to return total number of accesses for each url
	 *  for a given a set of urls, start hour and end hour.
	 *  
	 *  @throws InterruptedException, ParseException
     * @throws IOException 
	 */
    public void totalAccessRead(String startHour, String endHour) throws InterruptedException, ParseException, IOException 
   	{
    	try
    	{
	    	// sample urls for the query
	    	String url1 = "/english/individuals/player110745.htm";
	    	String url2 = "/french/images/news_bu_kits_on.gif";
	    	String url3 = "/english/history/past_cups/images/past_bu_86_off.gif";
	    	
	    	// store the url and hits for each url in the map
	    	Map<String, Integer> map = new HashMap<String, Integer>();
	    	map.put(url1, 0);
	    	map.put(url2, 0);
	    	map.put(url3, 0);
	    	
	   
	    	// parse string into date object
	    	Date start = dateFormat.parse(startHour);
	    	Date end = dateFormat.parse(endHour);
	    	String psString = "SELECT url, hour, hits FROM url_access WHERE url in (?, ?, ?)" +
	                   " AND hour > ? AND hour <= ?;";
	    	// prepared statement for querying the DB
	   		final PreparedStatement selectPS = session.prepare(psString);	
	   		BoundStatement bs = new BoundStatement(selectPS).bind(url1, url2, url3, start.getTime(), end.getTime());
	   		System.out.println(bs);
	   		
	    	// iterate through the result set and print the results on the console
	   		final ResultSetFuture queryFuture = session.executeAsync(bs);	
	   		ResultSet resultSet = queryFuture.getUninterruptibly();	   		
	   		for (Row row : resultSet)
	   		{
	   			System.out.println(row.getString(0) + " " + dateFormat.format(row.getLong(1)) + " " + row.getLong(2));
	   			
	   			// update the map
	   			int count = map.get(row.getString(0));
	   			count += row.getLong(2);
	   			map.put(row.getString(0), count);
	   		}
	   		
	   		for (Map.Entry<String, Integer> entry : map.entrySet()) {
	   		    System.out.println(entry.getKey() + ", " + entry.getValue());
	   		}
    	}
   		finally
		{
   			inputStreamReader.close();
			bufferedReader.close();
			session.shutdown();
		}
   		
   	}	
}
