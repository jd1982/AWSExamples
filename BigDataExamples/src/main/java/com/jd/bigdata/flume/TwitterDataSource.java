package com.jd.bigdata.flume;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

/**
 * A Flume Source, which pulls data from Twitter's streaming API. Currently,
 * this only supports pulling from the sample API, and only gets new status
 * updates.
 */
public class TwitterDataSource extends AbstractSource
    implements
		EventDrivenSource, Configurable {

	private static final Logger logger = LoggerFactory
			.getLogger(TwitterDataSource.class);

	/** Information necessary for accessing the Twitter API */
	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String accessTokenSecret;

	private String[] keywords;

	public Properties loadProperties() throws IOException {
		InputStream in = TwitterDataSource.class
				.getResourceAsStream("/bigdata.properties");
		Properties properties = new Properties();
		properties.load(in);
		return properties;
	}

	/** The actual Twitter stream. It's set up to collect raw JSON data */
	private final TwitterStream twitterStream = new TwitterStreamFactory(
			new ConfigurationBuilder().setJSONStoreEnabled(true).build())
			.getInstance();

	/**
	 * The initialization method for the Source. The context contains all the
	 * Flume configuration info, and can be used to retrieve any configuration
	 * values necessary to set up the Source.
	 */
	public void configure(Context context) {

		try {
			
			Properties properties = loadProperties();

			consumerKey = properties.getProperty("twitter.consumerkey");
			consumerSecret = properties.getProperty("twitter.consumersecret");
			accessToken = properties.getProperty("twitter.accessstoken");
			accessTokenSecret = properties.getProperty("twitter.accesstokensecret");
		} catch (IOException ex) {
			logger.error("Unable to load properties file. Do you have bigdata.properties on your classpath?", ex);
		}

		String keywordString = context.getString("london", "");
		
		keywords = keywordString.split(",");
		for (int i = 0; i < keywords.length; i++) {
			keywords[i] = keywords[i].trim();
		}
	}

	/**
	 * Start processing events. This uses the Twitter Streaming API to sample
	 * Twitter, and process tweets.
	 */
	@Override
	public void start() {
		// The channel is the piece of Flume that sits between the Source and
		// Sink,
		// and is used to process events.
		final ChannelProcessor channel = getChannelProcessor();

		final Map<String, String> headers = new HashMap<String, String>();

		// The StatusListener is a twitter4j API, which can be added to a
		// Twitter
		// stream, and will execute methods every time a message comes in
		// through
		// the stream.
		StatusListener listener = new StatusListener() {
			// The onStatus method is executed every time a new tweet comes in.
			public void onStatus(Status status) {
				// The EventBuilder is used to build an event using the headers
				// and
				// the raw JSON of a tweet
				logger.debug(status.getUser().getScreenName() + ": "
						+ status.getText());

				headers.put("timestamp",
						String.valueOf(status.getCreatedAt().getTime()));
				Event event = EventBuilder.withBody(DataObjectFactory
						.getRawJSON(status).getBytes(), headers);

				channel.processEvent(event);
			}

			// This listener will ignore everything except for new tweets
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			}

			public void onScrubGeo(long userId, long upToStatusId) {
			}

			public void onException(Exception ex) {
			}

			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}
		};

		logger.debug(
				"Setting up Twitter sample stream using consumer key {} and"
						+ " access token {}", new String[] { consumerKey,
						accessToken });
		// Set up the stream's listener (defined above), and set any necessary
		// security information.
		twitterStream.addListener(listener);
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);

		// Set up a filter to pull out industry-relevant tweets
		if (keywords.length == 0) {
			logger.debug("Starting up Twitter sampling...");
			FilterQuery query = new FilterQuery().track(new String[] {
					"london", "London" });
			twitterStream.sample();
		} else {
			logger.debug("Starting up Twitter filtering...");
			FilterQuery query = new FilterQuery().track(keywords);
			twitterStream.sample();
		}
		super.start();
	}

	/**
	 * Stops the Source's event processing and shuts down the Twitter stream.
	 */
	@Override
	public void stop() {
		logger.debug("Shutting down Twitter sample stream...");
		twitterStream.shutdown();
		super.stop();
	}
}
