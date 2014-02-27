package uk.ac.ncl.cs.csc8101.weblogcoursework;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FileUtil2 {
	
	public static String[] tokenizer(String str)
	{
		return str.split(" ");
	}
	
	
	public static Date parseDate(String str1, String str2) throws ParseException
	{
		String dateString = str1 + " " + str2;
		DateFormat dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]");
		Date date = dateFormat.parse(dateString);
       // long millis = date.getTime();
		
        return date;
	}
	
	public static String url(String str1, String str2, String str3)
	{
		return str1 + " " +  str2 + " " + str3;
	}
}



