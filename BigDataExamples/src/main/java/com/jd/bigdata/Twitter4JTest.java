package com.jd.bigdata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.UserMentionEntity;
import twitter4j.json.DataObjectFactory;

public class Twitter4JTest {

	public static void main(String args[]) throws Exception {
		new Twitter4JTest();
	}

	public Twitter4JTest() throws Exception {
		File file = new File("c:\\AWS\\twitter.txt");

		FileInputStream fis = new FileInputStream(file);
		InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
		BufferedReader reader = new BufferedReader(isr);

		String line = reader.readLine();

		while (line != null) {
			Status status = DataObjectFactory.createStatus(line);

			String s = " ";

			URLEntity[] urls = status.getURLEntities();

			for (URLEntity url : urls) {
				if (url.getExpandedURL().trim().length() > 0)
					s = s + url.getExpandedURL() + " ";
			}

			if (s.trim().length() > 0) {
				s = status.getText() + "   " + s;
			}

			HashtagEntity[] entities = status.getHashtagEntities();

			String hashTags = "";

			for (HashtagEntity entity : entities) {
				hashTags = hashTags + "#" + entity.getText() + " ";
			}

			
			String userMentions = "";
			UserMentionEntity[] userMentionEntities = status.getUserMentionEntities();
			
			for (UserMentionEntity entity : userMentionEntities) {
				userMentions = userMentions + entity.getScreenName() + " ";
			}
			
			System.out.println(status.getUser().getScreenName() + " >>>>> " + userMentions + " " + status.getInReplyToScreenName() + " --------------- " + status.getText());

			// System.out.println(status.getText() + " " + status.getCreatedAt()
			// + " " + status.getRetweetCount());
			// System.out.println(status.getPlace());
			// if(s.trim().length() > 0)
			// System.out.println("<" + status.getUser().getName() + " , " +
			// status.getUser().getFollowersCount() + ">     " + s + " " +
			// hashTags);
			line = reader.readLine();
		}

		reader.close();

		// Influentioal users
		// <username, retweet count>
		// <url, urser>

	}
}
