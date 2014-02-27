/*
Copyright 2014 Red Hat, Inc. and/or its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package uk.ac.ncl.cs.csc8101.weblogcoursework;

import com.datastax.driver.core.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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

/**
 * Write pipelining tests for cassandra server v2 / CQL3 via datastax
 * java-driver
 * 
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2014-01
 */
public class CassandraPipelineIT {

	private static Cluster cluster;
	private static Session session;
	private static final File dataDir = new File("D:/temp/");
	private static final File logFile = new File(dataDir, "loglite");
	private static InputStreamReader inputStreamReader;
	private static BufferedReader bufferedReader;

	@BeforeClass
	public static void staticSetup() {

		cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build();

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

		session.execute("CREATE TABLE IF NOT EXISTS user_table (id text, date timestamp, url text,"
				+ "PRIMARY KEY (id, date, url) )");

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
	public void pipelineWrites() throws InterruptedException, ParseException,
			IOException {

		final int maxOutstandingFutures = 4;
		final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(
				maxOutstandingFutures);

		final PreparedStatement insertPS = session
				.prepare("INSERT INTO user_table (id, date, url) "
						+ "VALUES (?, ?, ?)");
		int count = 0;
		String line = bufferedReader.readLine();
		while (line != null) {

			int itemsPerBatch = 0;
			final BatchStatement batchStatement = new BatchStatement(
					BatchStatement.Type.UNLOGGED);
			while (itemsPerBatch < 100) {

				if (line == null)
					break;
				String[] tokens = FileUtil2.tokenizer(line);
				String id = tokens[0];
				Date timeStamp = FileUtil2.parseDate(tokens[1], tokens[2]);
				String url = tokens[4];
				batchStatement.add(new BoundStatement(insertPS).bind(id,
						timeStamp, url));
				itemsPerBatch++;
				line = bufferedReader.readLine();
				System.out.println(++count);
			}
			outstandingFutures.put(session.executeAsync(batchStatement));

			if (outstandingFutures.remainingCapacity() == 0) {
				ResultSetFuture resultSetFuture = outstandingFutures.take();
				resultSetFuture.getUninterruptibly();
			}
			if (count > 2000000) break;
		}

		while (!outstandingFutures.isEmpty()) {
			ResultSetFuture resultSetFuture = outstandingFutures.take();
			resultSetFuture.getUninterruptibly();
		}
	}

	@Test
	public void activityBetweenTimes() throws InterruptedException, ParseException {
		String id = "3912";
		DateFormat dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]");
		Date startTime = dateFormat.parse("[01/Apr/1998:13:08:16 +0000]");
		Date endTime = dateFormat.parse("[01/Jun/1998:13:08:16 +0000]");

		//final int maxOutstandingFutures = 4;
		//final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(maxOutstandingFutures);
		final PreparedStatement selectPS = session
				.prepare("SELECT * FROM user_table WHERE id=? AND date > ? AND date <= ?;");
		final ResultSetFuture queryFuture = session
				.executeAsync(new BoundStatement(selectPS).bind(id, startTime,
						endTime));
		ResultSet resultSet = queryFuture.getUninterruptibly();
		System.out.println(resultSet.getAvailableWithoutFetching());
		for (Row row : resultSet) {
			System.out.println(row.getString(0) + " " + row.getDate(1) + " "
					+ row.getString(2));
		}
		
		
	}
}
