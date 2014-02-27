package uk.ac.ncl.cs.csc8101.weblogcoursework;

import java.io.File;
import java.io.PrintWriter;

public class TestImplementation {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try
		{
			/**** Test ActivitiesBetweenTimes class: uncomment a line to test it ***/
	
			ActivitiesBetweenTimes activities = new ActivitiesBetweenTimes();
			// query the DB
			String id = "182";
			String startTime = "[30/Apr/1998:22:17:59 +0000]";
			String endTime = "[30/Apr/1998:23:20:00 +0000]";
			activities.writeTodB();
			//activities.activityBetweenTimes(id, startTime, endTime);
			
	
			/*** Testing TotalNumberOfAccesses class: uncomment a line to test it ***/
	
			String startHour = "[05/Apr/1998:00]";
			String endHour = "[30/May/1998:22]";	
			TotalNumberOfAccesses total = new TotalNumberOfAccesses();
			total.writeToDB();	
			//total.totalAccessRead(startHour, endHour);
			
	
			/*** Testing UserSession class: uncomment a line to test it ***/
	
			UserSession session = new UserSession();
			session.writeSession();
			//session.queryUserSessions("9618");
			

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}

}
