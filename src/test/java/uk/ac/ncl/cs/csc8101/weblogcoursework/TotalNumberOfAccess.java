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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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

public class TotalNumberOfAccess 
{
	    private static Cluster cluster;
	    private static Session session;
	    private static final File dataDir = new File("D:/temp/");
		private static final File logFile = new File(dataDir, "loglite");
		private static InputStreamReader inputStreamReader;
	    private static BufferedReader bufferedReader;
	    private static  DateFormat dateFormat;

	    @BeforeClass
	    public static void staticSetup() {

	        cluster = new Cluster.Builder()
	                .addContactPoint("127.0.0.1")
	                .build();
	        
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

	        session.execute("CREATE TABLE IF NOT EXISTS url_access (url text, hour timestamp, hits counter, PRIMARY KEY ((url), hour));");
	       // session.execute("CREATE TABLE IF NOT EXISTS url_counter_table (url, v counter, PRIMARY KEY (k) )");
	        
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

	    @AfterClass
	    public static void staticCleanup() {
	        session.shutdown();
	        cluster.shutdown();
	        try {
				bufferedReader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }

	    @Test
		public void totalAccessWrite() throws InterruptedException
		{
			final int maxOutstandingFutures = 4;
			final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(
					maxOutstandingFutures);
			try 
			{	
				final PreparedStatement updatePS = session
						.prepare("UPDATE url_access SET hits = hits + ? WHERE url = ? AND hour = ?;");
				int count = 0;
				String line = bufferedReader.readLine();	
				while (line != null) 
				{					
					int itemsPerBatch = 0;
					final BatchStatement batchStatement = new BatchStatement(
							BatchStatement.Type.COUNTER);
					while (itemsPerBatch < 100) 
					{						
						if (line == null)
							break;
						String[] tokens = FileUtil2.tokenizer(line);	
						String date = tokens[1].substring(0, 15) + "]";
						Date timeStamp = (Date) dateFormat.parse(date);//FileUtil.parseDate(tokens[1], tokens[2]);
						String url = tokens[4];										 
						batchStatement.add(new BoundStatement(updatePS).bind(1L, url, timeStamp));
						itemsPerBatch++;
						line = bufferedReader.readLine();	
						System.out.println(++count);
					}
					outstandingFutures.put(session.executeAsync(batchStatement));
					
					if (outstandingFutures.remainingCapacity() == 0) {
						ResultSetFuture resultSetFuture = outstandingFutures.take();
						resultSetFuture.getUninterruptibly();
					}
					if (count > 2000) break;
				}
			}
			catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			while (!outstandingFutures.isEmpty()) {
				ResultSetFuture resultSetFuture = outstandingFutures.take();
				resultSetFuture.getUninterruptibly();
			}
		}

	    @Test
	   	public void totalAccessRead() throws InterruptedException, ParseException 
	   	{
	    	String url1 = "/images/hm_bg.jpg";
	    	String url2 = "/images/s102323.gif";
	    	String url3 = "/english/history/images/france98b.GIF";
	    	DateFormat dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH]");
	    	Date startHour = dateFormat.parse("[29/Apr/1998:00]");
	    	Date endHour = dateFormat.parse("[30/Apr/1998:22]");
	   		//final int maxOutstandingFutures = 4;
	   		//final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(maxOutstandingFutures);	
	    	String psString = "SELECT url, hits FROM url_access WHERE url in ('" + url1 + "', '" + url2 + "', '" + url3 + "')" +
                       " AND hour > ? AND hour <= ?;";
	    	
	   		final PreparedStatement selectPS = session.prepare(psString);	
	   		BoundStatement bs = new BoundStatement(selectPS).bind(startHour, endHour);
	   		System.out.println(bs);
	   		
	   		final ResultSetFuture queryFuture = session.executeAsync(bs);	
	   		ResultSet resultSet = queryFuture.getUninterruptibly();	   		
	   		for (Row row : resultSet)
	   		{
	   			System.out.println(row.getString(0) + " " + row.getLong(1));
	   		}
	   		
	   		System.out.println(resultSet.getAvailableWithoutFetching());
	   	}	
}
