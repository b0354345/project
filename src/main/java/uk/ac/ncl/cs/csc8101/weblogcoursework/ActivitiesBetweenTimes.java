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
 * This class creates a schema taht can be used to query user's activities
 * between times.
 * @author Saleh Mohamed (103543457)
 * @version 18th February 2014
 */
public class ActivitiesBetweenTimes {
	
	private static Cluster cluster;
	private static Session session;
//	private static final File dataDir = new File("D:/temp/");
	private static final File dataDir = new File("/home/ubuntu/data/cassandra-test-dataset");
	private static final File logFile = new File(dataDir, "CSC8101-logfile.gz");
	private static InputStreamReader inputStreamReader;
	private static BufferedReader bufferedReader;
	private static DateFormat dateFormat;

	/**
	 * Public constructor for creating instances of this class
	 */
	public ActivitiesBetweenTimes()
	{
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
				.execute("CREATE KEYSPACE IF NOT EXISTS coursework WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
		bootstrapSession.shutdown();

		session = cluster.connect("coursework");

		session.execute("CREATE TABLE IF NOT EXISTS activities (id text, date bigint, url text,"
				+ "PRIMARY KEY (id, date, url) )");

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
	 */
	public void writeTodB() throws InterruptedException, ParseException, IOException
	{
		final int maxOutstandingFutures = 4;
		final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(
				maxOutstandingFutures);
		
		// prepared statement for inserting records into the table
		final PreparedStatement insertPS = session
				.prepare("INSERT INTO activities (id, date, url) "
						+ "VALUES (?, ?, ?)");
		int count = 0;
		String line = bufferedReader.readLine(); // read first line from the log file
		
		try
		{
			// stop the loop after reading the last line from the log file
			while (line != null) {
			
				int itemsPerBatch = 0;
				final BatchStatement batchStatement = new BatchStatement(
						BatchStatement.Type.UNLOGGED);
				
				 // stop this loop when the batch is full
				while (itemsPerBatch < 100) {
			
					if (line == null)
						break;
					
					// read a line and extract user id, timestamp and and url from it. 
					String[] tokens = line.split(" ");
					String id = tokens[0];
					Date timeStamp = dateFormat.parse(tokens[1] + " " + tokens[2]);
					String url = tokens[4];
					batchStatement.add(new BoundStatement(insertPS).bind(id,
							timeStamp.getTime(), url));
					itemsPerBatch++;
					line = bufferedReader.readLine();   // read next line
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
		finally
		{
			bufferedReader.close();
			session.shutdown();
		}
	}
	
	/**
	 *  This method sends a query to a DB to return user's activities between times
	 *  
	 *  @throws InterruptedException, ParseException
	 * @throws IOException 
	 */
	public void activityBetweenTimes(String id, String start, String end) throws InterruptedException, ParseException, IOException 
	{
		try
		{
			// parse string into date object
			Date startTime = dateFormat.parse(start);
			Date endTime = dateFormat.parse(end);
			
			// prepared statement for querying the DB
			final PreparedStatement selectPS = session
					.prepare("SELECT * FROM activities WHERE id=? AND date > ? AND date <= ?;");
			final ResultSetFuture queryFuture = session
					.executeAsync(new BoundStatement(selectPS).bind(id, startTime.getTime(),
							endTime.getTime()));
			
			// iterate through the result set and print the results on the console
			ResultSet resultSet = queryFuture.getUninterruptibly();
			SimpleDateFormat format = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]");
			for (Row row : resultSet) {
				System.out.println(row.getString(0) + " " + format.format(row.getLong(1)) + " "
						+ row.getString(2));
			}	
		}
		finally
		{
			bufferedReader.close();
			session.shutdown();
		}
	}
}
