package org.ohmage.domain;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonGenerator.Feature;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.ohmage.annotator.Annotator.ErrorCode;
import org.ohmage.exception.DomainException;
import org.ohmage.exception.InvalidRequestException;
import org.ohmage.exception.ValidationException;
import org.ohmage.request.UserRequest;
import org.ohmage.request.UserRequest.TokenLocation;
import org.ohmage.request.observer.StreamReadRequest;
import org.ohmage.request.observer.StreamReadRequest.ColumnNode;
import org.ohmage.request.omh.OmhReadResponder;
import org.ohmage.request.omh.OmhReadRunKeeperRequest;

/**
 * This class represents a payload ID for Run Keeper.
 *
 * @author John Jenkins
 */
public class RunKeeperPayloadId implements PayloadId {
	public static final String DOMAIN_ID = "run_keeper";
	
	/**
	 * The superclass for all Health Graph APIs.
	 *
	 * @author John Jenkins
	 */
	public abstract static class RunKeeperApi implements OmhReadResponder {
		/**
		 * The base URL for the RunKeeper (Health Graph) APIs. This does not 
		 * contain a path and one should be added from the 
		 * {@link RunKeeperApi}s.
		 */
		public static final String BASE_URL = "https://api.runkeeper.com/";
		
		/**
		 * The pattern for parsing and writing the date and time value for the
		 * user's birthday value. 
		 * 
		 * @see DATE_TIME_FORMATTER
		 */
		protected static final String DATE_TIME_REQUEST_FORMAT_STRING = 
			"y-M-d";
		/**
		 * The formatter that is used to parse and print the date time based on
		 * the {@link #DATE_TIME_REQUEST_FORMAT_STRING request format string}. 
		 * 
		 * @see DATE_TIME_REQUEST_FORMAT_STRING
		 */
		protected static final DateTimeFormatter DATE_TIME_REQUEST_FORMATTER = 
			DateTimeFormat.forPattern(DATE_TIME_REQUEST_FORMAT_STRING);
		
		/**
		 * The pattern for parsing and writing the date and time value for the
		 * user's birthday value. 
		 * 
		 * @see DATE_TIME_FORMATTER
		 */
		protected static final String DATE_TIME_RESPONSE_FORMAT_STRING = 
			"E, d MMM y HH:mm:ss";
		/**
		 * The formatter that is used to parse and print the date time based on
		 * the
		 * {@link #DATE_TIME_RESPONSE_FORMAT_STRING response format string}. 
		 * 
		 * @see DATE_TIME_RESPONSE_FORMAT_STRING
		 */
		protected static final DateTimeFormatter DATE_TIME_RESPONSE_FORMATTER = 
			DateTimeFormat.forPattern(DATE_TIME_RESPONSE_FORMAT_STRING);
		
		/**
		 * A JSON factory to be used by the sub-classes.
		 */
		protected static final JsonFactory JSON_FACTORY = 
			(new MappingJsonFactory())
				.configure(Feature.AUTO_CLOSE_JSON_CONTENT, true)
				.configure(Feature.AUTO_CLOSE_TARGET, true);
		
		/**
		 * The path to be used with the base URL.
		 */
		private final String path;
		
		/**
		 * A flag to indicate if this request has been made yet or not.
		 */
		private boolean madeRequest = false;
		
		/**
		 * Builds a base {@link RunKeeperApi RunKeeper API} with its default
		 * path from the {@link #BASE_URL base URL}.
		 * 
		 * @param path The path after the {@link #BASE_URL base URL}.
		 * 
		 * @throws IllegalArgumentException The path is null or only 
		 * 									whitespace.
		 */
		private RunKeeperApi(final String path) {
			if(path == null) {
				throw new IllegalArgumentException("The path is null.");
			}
			else if(path.trim().length() == 0) {
				throw new IllegalArgumentException(
					"The path is all whitespace.");
			}
			
			this.path = path;
		}
		
		/**
		 * Builds the fully-qualified URI for this path based on the 
		 * {@link #BASE_URL base URL}.
		 * 
		 * @return A URI built from the {@link #BASE_URL base URL} and this 
		 * 		   path.
		 * 
		 * @throws DomainException There was a problem creating the URI.
		 */
		public final URI getUri() throws DomainException {
			try {
				return new URI(BASE_URL + path);
			}
			catch(URISyntaxException e) {
				throw new DomainException(
					"The URL and/or path don't form a valid URI: " + 
						BASE_URL + path);
			}
		}
		
		/**
		 * Returns the path for this API.
		 * 
		 * @return The path for this API.
		 */
		public abstract String getPath();

		/**
		 * Returns whether or not the records at this API will have an ID 
		 * associated with each one.
		 * 
		 * @return Whether or not these records have IDs.
		 */
		public abstract boolean hasId();
		
		/**
		 * Returns whether or not the records at this API will have a timestamp
		 * associated with each one.
		 * 
		 * @return Whether or not these records have timestamps.
		 */
		public abstract boolean hasTimestamp();
		
		/**
		 * Returns whether or not the records at this API will have a location
		 * associated with each one.
		 * 
		 * @return Whether or not these records have locations.
		 */
		public abstract boolean hasLocation();
		
		/**
		 * Creates the registry entry for this RunKeeper API.
		 * 
		 * @param generator The generator to use to write the definition.
		 * 
		 * @throws JsonGenerationException There was an error creating the 
		 * 								   JSON.
		 * 
		 * @throws IOException There was an error writing to the generator.
		 */
		public void writeRegistryEntry(
				final JsonGenerator generator)
				throws JsonGenerationException, IOException {
			
			// RunKeeper definition
			generator.writeStartObject();
			
			// Output the chunk size which will be the same for all 
			// observers.
			generator.writeNumberField(
				"chunk_size", 
				StreamReadRequest.MAX_NUMBER_TO_RETURN);
			
			// There are no external IDs yet. This may change to
			// link to observer/read, but there are some
			// discrepancies in the parameters.
			
			// Set the local timezone as authoritative.
			generator.writeBooleanField(
				"local_tz_authoritative",
				true);
			
			// Set the summarizable as false for the time being.
			generator.writeBooleanField("summarizable", false);
			
			// Set the payload ID.
			StringBuilder payloadIdBuilder = 
				new StringBuilder("omh:" + DOMAIN_ID + ":");
			payloadIdBuilder.append(path);
			generator.writeStringField(
				"payload_id", 
				payloadIdBuilder.toString());
			
			// Set the payload version. For now, all surveys have 
			// the same version, 1.
			generator.writeStringField(
				"payload_version", 
				"1");
			
			// Set the payload definition.
			generator.writeFieldName("payload_definition"); 
			toConcordia(generator);

			// End the RunKeeper definition.
			generator.writeEndObject();
		}
		
		/**
		 * Generates the Concordia schema for this path.
		 * 
		 * @param generator The generator to use to write the definition.
		 * 
		 * @return The 'generator' that was passed in to facilitate chaining.
		 * 
		 * @throws JsonGenerationException There was a problem generating the
		 * 								   JSON.
		 * 
		 * @throws IOException There was a problem writing to the generator.
		 */
		public abstract JsonGenerator toConcordia(
			final JsonGenerator generator)
			throws JsonGenerationException, IOException;
		
		/**
		 * Makes the request to the API and stores the received data.
		 * 
		 * @param bearer The "Bearer" token generated by RunKeeper.
		 * 
		 * @param startDate Limits the data to only those points on or after
		 * 					this date and time.
		 * 
		 * @param endDate Limits the data to only those points on or before 
		 * 				  this date and time.
		 * 
		 * @param numToSkip This represents the number of records that will be
		 * 					skipped. Records are in reverse-chronological
		 * 					order.
		 * 
		 * @param numToReturn This represents the number of records that will 
		 * 					  be returned. This is processed after records have
		 * 					  been skipped.
		 * 
		 * @throws DomainException There was an error making the call.
		 */
		public final void service(
				final String bearer,
				final DateTime startDate,
				final DateTime endDate,
				final long numToSkip,
				final long numToReturn)
				throws DomainException {
			
			if(madeRequest) {
				return;
			}
			
			makeRequest(bearer, startDate, endDate, numToSkip, numToReturn);
			
			madeRequest = true;
		}
		
		/**
		 * Makes the request to the API and stores the received data. This will
		 * be called while the
		 * {@link #service(String, DateTime, DateTime, long, long)} call is
		 * being made. This allows the superclass to do some work before the
		 * subclasses make their call.
		 * 
		 * @param bearer The "Bearer" token generated by RunKeeper.
		 * 
		 * @param startDate Limits the data to only those points on or after
		 * 					this date and time.
		 * 
		 * @param endDate Limits the data to only those points on or before 
		 * 				  this date and time.
		 * 
		 * @param numToSkip This represents the number of records that will be
		 * 					skipped. Records are in reverse-chronological
		 * 					order.
		 * 
		 * @param numToReturn This represents the number of records that will 
		 * 					  be returned. This is processed after records have
		 * 					  been skipped.
		 * 
		 * @throws DomainException There was an error making the call.
		 */
		protected abstract void makeRequest(
			final String bearer,
			final DateTime startDate,
			final DateTime endDate,
			final long numToSkip,
			final long numToReturn)
			throws DomainException;
		
		/**
		 * Builds and makes the HTTP GET request. This will return the data as
		 * a string.
		 * 
		 * @param bearer The "Bearer" token generated by RunKeeper.
		 * 
		 * @param params The HTTP parameters to add to the request.
		 * 
		 * @return The response from RunKeeper as a String.
		 * 
		 * @throws DomainException There was a problem making the request.
		 */
		protected final String makeRequest(
				final String bearer,
				final Map<String, String> params)
				throws DomainException {

			StringBuilder uriBuilder = new StringBuilder(getUri().toString());
			
			// Add the parameters manually.
			String encoding = "UTF-8";
			if(params != null) {
				boolean firstPass = true;
				for(String key : params.keySet()) {
					if(firstPass) {
						uriBuilder.append('?');
						firstPass = false;
					}
					else {
						uriBuilder.append('&');
					}
					
					try {
						uriBuilder.append(URLEncoder.encode(key, encoding));
						uriBuilder.append('=');
						uriBuilder
							.append(
								URLEncoder.encode(params.get(key), encoding));
					}
					catch(UnsupportedEncodingException e) {
						throw 
							new DomainException(
								"The encoding is unknown: " + encoding);
					}
				}
			}
			
			HttpGet httpGet = new HttpGet(uriBuilder.toString());
			httpGet.addHeader("Authorization", "Bearer " + bearer);
			
			HttpClient httpClient = new DefaultHttpClient();
			HttpResponse httpResponse;
			try {
				httpResponse = httpClient.execute(httpGet);
			}
			catch(ClientProtocolException e) {
				throw new DomainException("There was an HTTP error.", e);
			}
			catch(IOException e) {
				throw new DomainException(
					"There was an error communicating with the server.",
					e);
			}
			
			try {
				return 
					(new BasicResponseHandler()).handleResponse(httpResponse);
			}
			catch(HttpResponseException e) {
				throw new DomainException(
					"The server returned an error.",
					e);
			}
			catch(IOException e) {
				throw new DomainException(
					"There was an error commmunicating with the server.",
					e);
			}
		}
	};
	
	/**
	 * A {@link RunKeeperApi} for the user's profile.
	 *
	 * @author John Jenkins
	 */
	public static class ProfileApi extends RunKeeperApi {
		/**
		 * The URL's path to the profile. Should be used in conjunction with 
		 * the {@link #BASE_URL base URL}.
		 */
		private static final String PATH = "profile";

		/**
		 * The 'birthday' parameter field name.
		 */
		private static final String JSON_KEY_BIRTHDAY = "birthday";
		/**
		 * The 'location' parameter field name.
		 */
		private static final String JSON_KEY_LOCATION = "location";
		/**
		 * The 'name' parameter field name.
		 */
		private static final String JSON_KEY_NAME = "name";
		/**
		 * The 'elite' parameter field name.
		 */
		private static final String JSON_KEY_ELITE = "elite";
		/**
		 * The 'gender' parameter field name.
		 */
		private static final String JSON_KEY_GENDER = "gender";
		/**
		 * The 'athlete_type' parameter field name.
		 */
		private static final String JSON_KEY_ATHLETE_TYPE = "athlete_type";
		/**
		 * The 'profile' parameter field name.
		 */
		private static final String JSON_KEY_PROFILE = "profile";
		
		private DateTime birthday = null;
		private String location = null;
		private String name = null;
		private String elite = null;
		private String gender = null;
		private String athleteType = null;
		private String profile = null;
		private String userId = null;
		
		/**
		 * Creates a {@link RunKeeperApi} to the user's RunKeeper profile.
		 */
		public ProfileApi() {
			super(PATH);
		}
		
		/*
		 * (non-Javadoc)
		 * @see org.ohmage.domain.RunKeeperPayloadId.RunKeeperApi#getPath()
		 */
		public String getPath() {
			return PATH;
		}

		/*
		 * (non-Javadoc)
		 * @see org.ohmage.domain.RunKeeperPayloadId.RunKeeperApi#hasId()
		 */
		@Override
		public boolean hasId() {
			return true;
		}

		/*
		 * (non-Javadoc)
		 * @see org.ohmage.domain.RunKeeperPayloadId.RunKeeperApi#hasTimestamp()
		 */
		@Override
		public boolean hasTimestamp() {
			return false;
		}

		/*
		 * (non-Javadoc)
		 * @see org.ohmage.domain.RunKeeperPayloadId.RunKeeperApi#hasLocation()
		 */
		@Override
		public boolean hasLocation() {
			return false;
		}
		
		/*
		 * (non-Javadoc)
		 * @see org.ohmage.domain.RunKeeperPayloadId.RunKeeperApi#toConcordia(org.codehaus.jackson.JsonGenerator)
		 */
		@Override
		public final JsonGenerator toConcordia(
				final JsonGenerator generator)
				throws JsonGenerationException, IOException {
			
			// Begin the definition.
			generator.writeStartObject();
			
			// The type of the data is a JSON object.
			generator.writeStringField("type", "object");
			
			// Define that object.
			generator.writeArrayFieldStart("schema");
			
			// Add the "birthday" field.
			generator.writeStartObject();
			generator.writeStringField("name", JSON_KEY_BIRTHDAY);
			generator.writeStringField("type", "string");
			generator.writeEndObject();
			
			// Add the "location" field.
			generator.writeStartObject();
			generator.writeStringField("name", JSON_KEY_LOCATION);
			generator.writeStringField("type", "string");
			generator.writeEndObject();
			
			// Add the "name" field.
			generator.writeStartObject();
			generator.writeStringField("name", JSON_KEY_NAME);
			generator.writeStringField("type", "string");
			generator.writeEndObject();
			
			// Add the "elite" field. Not sure why they decided to quote the
			// boolean, but there it is.
			generator.writeStartObject();
			generator.writeStringField("name", JSON_KEY_ELITE);
			generator.writeStringField("type", "string");
			generator.writeEndObject();
			
			// Add the "gender" field.
			generator.writeStartObject();
			generator.writeStringField("name", JSON_KEY_GENDER);
			generator.writeStringField("type", "string");
			generator.writeEndObject();
			
			// Add the "athlete_type" field.
			generator.writeStartObject();
			generator.writeStringField("name", JSON_KEY_ATHLETE_TYPE);
			generator.writeStringField("type", "string");
			generator.writeEndObject();
			
			// Add the "profile" field.
			generator.writeStartObject();
			generator.writeStringField("name", JSON_KEY_PROFILE);
			generator.writeStringField("type", "string");
			generator.writeEndObject();
			
			// Finish defining the overall object.
			generator.writeEndArray();
			
			// Close the definition.
			generator.writeEndObject();
			
			// Return the generator to facilitate chaining.
			return generator;
		}

		/**
		 * @param startDate This parameter is ignored.
		 * 
		 * @param endDate This parameter is ignored.
		 * 
		 * @param numToSkip This parameter is ignored.
		 * 
		 * @param numToReturn This parameter is ignored.
		 */
		@Override
		public void makeRequest(
				final String bearer,
				final DateTime startDate,
				final DateTime endDate,
				final long numToSkip,
				final long numToReturn)
				throws DomainException {
			
			// Get the API's response.
			String result = makeRequest(bearer, null);
			
			// Process the API's response.
			try {
				JsonParser parser = JSON_FACTORY.createJsonParser(result);

				if(parser.nextToken() != JsonToken.START_OBJECT) {
					throw 
						new DomainException(
							"The response was not a JSON object.");
				}
				
				while(parser.nextToken() != JsonToken.END_OBJECT) {
					// Get the field's name and point to its value.
					String fieldName = parser.getCurrentName();
					parser.nextToken();
					
					// This is a cheat because all fields' value is a string.
					String fieldValue = parser.getText();
					
					// Switch on the field name and assign the appropriate 
					// value.
					if(JSON_KEY_BIRTHDAY.equals(fieldName)) {
						try {
							birthday =
								DATE_TIME_RESPONSE_FORMATTER
									.parseDateTime(fieldValue);
						}
						catch(IllegalArgumentException e) {
							throw new DomainException(
								"The date/time value for the birthday could not be parsed: " +
									fieldValue,
								e);
						}
					}
					else if(JSON_KEY_LOCATION.equals(fieldName)) {
						location = fieldValue;
					}
					else if(JSON_KEY_NAME.equals(fieldName)) {
						name = fieldValue;
					}
					else if(JSON_KEY_ELITE.equals(fieldName)) {
						elite = fieldValue;
					}
					else if(JSON_KEY_GENDER.equals(fieldName)) {
						gender = fieldValue;
					}
					else if(JSON_KEY_ATHLETE_TYPE.equals(fieldName)) {
						athleteType = fieldValue;
					}
					else if(JSON_KEY_PROFILE.equals(fieldName)) {
						profile = fieldValue;
						
						String[] profileParts = profile.split("/");
						userId = profileParts[profileParts.length - 1];
					}
					// Otherwise, it was a value we didn't understand and will
					// ignore for now.
				}
			}
			catch(JsonParseException e) {
				throw new DomainException("The result was not valid JSON.", e);
			}
			catch(IOException e) {
				throw new DomainException("Could not read the result.", e);
			}
		}

		/**
		 * @return Always 1 because the user will only have one profile.
		 */
		@Override
		public long getNumDataPoints() {
			return 1;
		}
		
		/*
		 * (non-Javadoc)
		 * @see org.ohmage.request.omh.OmhReadResponder#respond(org.codehaus.jackson.JsonGenerator, org.ohmage.request.observer.StreamReadRequest.ColumnNode)
		 */
		@Override
		public void respond(
				final JsonGenerator generator,
				final ColumnNode<String> columns)
				throws JsonGenerationException, IOException, DomainException {
			
			// Write each point which, in this case, will only be the one point
			// representing the user's information.
			generator.writeStartObject();
			
			// Write the metadata.
			generator.writeObjectFieldStart("metadata");
			
			// Write the user's ID.
			generator.writeStringField("id", userId);
			
			// End the metadata.
			generator.writeEndObject();
			
			// Write the data.
			generator.writeObjectFieldStart("data");
			
			// Determine if all columns need to be output.
			boolean allColumns = (columns == null) || columns.isLeaf();
			
			// If applicable, output the 'birthday' column.
			if(allColumns || columns.hasChild(JSON_KEY_BIRTHDAY)) {
				generator
					.writeStringField(
						JSON_KEY_BIRTHDAY,
						DATE_TIME_RESPONSE_FORMATTER
							.print(birthday));
			}

			// If applicable, output the 'location' column.
			if(allColumns || columns.hasChild(JSON_KEY_LOCATION)) {
				generator.writeStringField(JSON_KEY_LOCATION, location);
			}

			// If applicable, output the 'name' column.
			if(allColumns || columns.hasChild(JSON_KEY_NAME)) {
				generator.writeStringField(JSON_KEY_NAME, name);
			}

			// If applicable, output the 'elite' column.
			if(allColumns || columns.hasChild(JSON_KEY_ELITE)) {
				generator.writeStringField(JSON_KEY_ELITE, elite);
			}

			// If applicable, output the 'gender' column.
			if(allColumns || columns.hasChild(JSON_KEY_GENDER)) {
				generator.writeStringField(JSON_KEY_GENDER, gender);
			}

			// If applicable, output the 'athlete_type' column.
			if(allColumns || columns.hasChild(JSON_KEY_ATHLETE_TYPE)) {
				generator.writeStringField(JSON_KEY_ATHLETE_TYPE, athleteType);
			}

			// If applicable, output the 'profile' column.
			if(allColumns || columns.hasChild(JSON_KEY_PROFILE)) {
				generator.writeStringField(JSON_KEY_PROFILE, profile);
			}
			
			// End the data.
			generator.writeEndObject();
			
			// End the only data point.
			generator.writeEndObject();
		}
	}
	
	/**
	 * A {@link RunKeeperApi} for the user's fitness activities.
	 *
	 * @author John Jenkins
	 */
	public static class FitnessActivitiesApi extends RunKeeperApi {
		/**
		 * The URL's path to the profile. Should be used in conjunction with 
		 * the {@link #BASE_URL base URL}.
		 */
		private static final String PATH = "fitnessActivities";
		
		/**
		 * The number of records to skip _at the end of the returned list_. If
		 * we are returned 10 records and this value is 3, we return the
		 * _first_ 7 records.
		 */
		private long numToSkip = 0;
		
		/**
		 * This class represents a single data point returned from the API.
		 *
		 * @author John Jenkins
		 */
		private static final class Result {
			private static final String JSON_KEY_TYPE = "type";
			private static final String JSON_KEY_START_TIME = "start_time";
			private static final String JSON_KEY_TOTAL_DISTANCE = 
				"total_distance";
			private static final String JSON_KEY_DURATION = "duration";
			private static final String JSON_KEY_URI = "uri";
			
			private String id;
			private String type;
			private DateTime startTime;
			private double totalDistance;
			private double duration;
			private String uri;

			/**
			 * Generates the Concordia schema for this path.
			 * 
			 * @param generator The generator to use to write the definition.
			 * 
			 * @return The 'generator' that was passed in to facilitate
			 * 		   chaining.
			 * 
			 * @throws JsonGenerationException There was a problem generating 
			 * 								   the JSON.
			 * 
			 * @throws IOException There was a problem writing to the 
			 * 					   generator.
			 */
			public static JsonGenerator toConcordia(
					final JsonGenerator generator)
					throws JsonGenerationException, IOException {
				
				// Start the definition.
				generator.writeStartObject();
				
				// The data will always be a JSON object.
				generator.writeStringField("type", "object");
				generator.writeArrayFieldStart("schema");
				
				// Add the 'duration' field.
				generator.writeStartObject();
				generator.writeStringField("name", JSON_KEY_DURATION);
				generator.writeStringField("type", "number");
				generator.writeEndObject();
				
				// Add the 'start_time' field.
				generator.writeStartObject();
				generator.writeStringField("name", JSON_KEY_START_TIME);
				generator.writeStringField("type", "string");
				generator.writeEndObject();
				
				// Add the 'total_distance' field.
				generator.writeStartObject();
				generator.writeStringField("name", JSON_KEY_TOTAL_DISTANCE);
				generator.writeStringField("type", "number");
				generator.writeEndObject();
				
				// Add the 'type' field.
				generator.writeStartObject();
				generator.writeStringField("name", JSON_KEY_TYPE);
				generator.writeStringField("type", "string");
				generator.writeEndObject();
				
				// Add the 'uri' field.
				generator.writeStartObject();
				generator.writeStringField("name", JSON_KEY_URI);
				generator.writeStringField("type", "string");
				generator.writeEndObject();
				
				// End the overall schema array.
				generator.writeEndArray();
				
				// End the definition.
				generator.writeEndObject();
				
				// Return the generator.
				return generator;
			}
		}
		List<Result> results = new LinkedList<Result>();
		
		/**
		 * Creates a {@link RunKeeperApi} to the user's fitness activities.
		 */
		public FitnessActivitiesApi() {
			super(PATH);
		}
		
		/*
		 * (non-Javadoc)
		 * @see org.ohmage.domain.RunKeeperPayloadId.RunKeeperApi#getPath()
		 */
		public String getPath() {
			return PATH;
		}

		/*
		 * (non-Javadoc)
		 * @see org.ohmage.domain.RunKeeperPayloadId.RunKeeperApi#hasId()
		 */
		@Override
		public boolean hasId() {
			return true;
		}

		/*
		 * (non-Javadoc)
		 * @see org.ohmage.domain.RunKeeperPayloadId.RunKeeperApi#hasTimestamp()
		 */
		@Override
		public boolean hasTimestamp() {
			return true;
		}

		/*
		 * (non-Javadoc)
		 * @see org.ohmage.domain.RunKeeperPayloadId.RunKeeperApi#hasLocation()
		 */
		@Override
		public boolean hasLocation() {
			return false;
		}

		/*
		 * (non-Javadoc)
		 * @see org.ohmage.domain.RunKeeperPayloadId.RunKeeperApi#toConcordia(org.codehaus.jackson.JsonGenerator)
		 */
		@Override
		public JsonGenerator toConcordia(
				final JsonGenerator generator)
				throws JsonGenerationException, IOException {

			// Return the generator.
			return Result.toConcordia(generator);
		}

		/*
		 * (non-Javadoc)
		 * @see org.ohmage.domain.RunKeeperPayloadId.RunKeeperApi#makeRequest(java.lang.String, org.joda.time.DateTime, org.joda.time.DateTime, long, long)
		 */
		@Override
		public void makeRequest(
				final String bearer,
				final DateTime startDate,
				final DateTime endDate,
				final long numToSkip,
				final long numToReturn)
				throws DomainException {
			
			Map<String, String> params = new HashMap<String, String>();
			if(startDate != null) {
				params
					.put(
						"noEarlierThan",
						DATE_TIME_REQUEST_FORMATTER.print(startDate));
			}
			if(endDate != null) {
				params
					.put(
						"noLaterThan",
						DATE_TIME_REQUEST_FORMATTER.print(endDate));
			}
			
			// Calculate the number of records to skip and return. We may end
			// up getting back more than we wanted, but the 'this.numToSkip'
			// will indicate how many to omit.
			if(numToReturn != 0) {
				this.numToSkip = numToSkip % numToReturn;
				params.put("page", Long.toString(numToSkip / numToReturn));
			}
			else {
				params.put("page", "0");
			}
			params
				.put("pageSize", Long.toString(numToReturn + this.numToSkip));
			
			String resultString = makeRequest(bearer, params);
			// Process the API's response.
			try {
				JsonParser parser = 
					JSON_FACTORY.createJsonParser(resultString);
	
				// Ensure that the response is a JSON object.
				if(parser.nextToken() != JsonToken.START_OBJECT) {
					throw 
						new DomainException(
							"The response was not a JSON object.");
				}
				
				// FIXME: This is buggy and requires more careful processing.
				// The issue is, there is an assumption that the resulting data
				// will not vary from what is expected from this code. If the
				// data's format were to vary, the results are undefined.
				//
				// While we have not reached the end of the response.
				while(parser.nextToken() != null) {
					// Get the field's name and point to its value.
					String fieldName = parser.getCurrentName();
					
					// Get the array of result points.
					if("items".equals(fieldName)) {
						if(parser.nextToken() != JsonToken.START_ARRAY) {
							throw new DomainException(
								"The 'items' field was not a JSON array.");
						}
						
						// Loop through each index.
						JsonToken currToken;
						while((currToken = parser.nextToken()) != JsonToken.END_ARRAY) {
							// The data at each index must be an object.
							if(currToken != JsonToken.START_OBJECT) {
								throw new DomainException(
									"The array element is not a JSON object: " +
										currToken.toString());
							}
							
							// Create the new Result object.
							Result currResult = new Result();
							
							// Loop through all of the elements in the object.
							while(parser.nextToken() != JsonToken.END_OBJECT) {
								// Get the field's name.
								String currFieldName = parser.getCurrentName();
								
								// Advance the pointer to the field's value.
								parser.nextToken();
								
								if(Result.JSON_KEY_DURATION.equals(currFieldName)) {
									currResult.duration = 
										parser.getNumberValue().doubleValue(); 
								}
								else if(Result.JSON_KEY_START_TIME.equals(currFieldName)) {
									currResult.startTime = 
										DATE_TIME_RESPONSE_FORMATTER
											.parseDateTime(parser.getText());
								}
								else if(Result.JSON_KEY_TOTAL_DISTANCE.equals(currFieldName)) {
									currResult.totalDistance =
										parser.getNumberValue().doubleValue();
								}
								else if(Result.JSON_KEY_TYPE.equals(currFieldName)) {
									currResult.type = parser.getText();
								}
								else if(Result.JSON_KEY_URI.equals(currFieldName)) {
									currResult.uri = parser.getText();
									
									String[] uriParts = 
										currResult.uri.split("/");
									currResult.id = 
										uriParts[uriParts.length - 1];
								}
							}

							// If it is not before the start date or after the
							// end date, add it to the results.
							if(!(	(startDate != null) &&
									(startDate.isAfter(currResult.startTime))
								) ||
								(	(endDate != null) &&
									(endDate.isBefore(currResult.startTime)))) {
								
								results.add(currResult);
							}
						}
					}
					// Otherwise, it was a value we didn't understand and will
					// ignore for now.
				}
			}
			catch(JsonParseException e) {
				throw new DomainException("The result was not valid JSON.", e);
			}
			catch(IOException e) {
				throw new DomainException("Could not read the result.", e);
			}
		}

		/*
		 * (non-Javadoc)
		 * @see org.ohmage.request.omh.OmhReadResponder#getNumDataPoints()
		 */
		@Override
		public long getNumDataPoints() {
			return results.size();
		}

		/*
		 * (non-Javadoc)
		 * @see org.ohmage.request.omh.OmhReadResponder#respond(org.codehaus.jackson.JsonGenerator, org.ohmage.request.observer.StreamReadRequest.ColumnNode)
		 */
		@Override
		public void respond(
				final JsonGenerator generator,
				final ColumnNode<String> columns)
				throws JsonGenerationException, IOException, DomainException {

			// Create the reusable DateTimeFormatter. 
			DateTimeFormatter isoDateTimeFormatter = 
				ISODateTimeFormat.dateTime();
			
			// For each object,
			for(Result result : results) {
				// Start the overall object.
				generator.writeStartObject();
				
				// Write the metadata.
				generator.writeObjectFieldStart("metadata");
				
				// Write the ID.
				generator.writeStringField("id", result.id);
				
				// Write the timestamp.
				generator
					.writeStringField(
						"timestamp", 
						isoDateTimeFormatter.print(result.startTime));
				
				// End the metadata object.
				generator.writeEndObject();
				
				// Write the data.
				generator.writeObjectFieldStart("data");
				
				// Determine if all columns are being returned.
				boolean allColumns = (columns == null) || columns.isLeaf();
				
				// Write the 'duration' field.
				if(allColumns || columns.hasChild(Result.JSON_KEY_DURATION)) {
					generator
						.writeNumberField(
							Result.JSON_KEY_DURATION,
							result.duration);
				}
				
				// Write the 'start_time' field.
				if(allColumns || columns.hasChild(Result.JSON_KEY_START_TIME)) {
					generator
						.writeStringField(
							Result.JSON_KEY_START_TIME,
							DATE_TIME_RESPONSE_FORMATTER
								.print(result.startTime));
				}
				
				// Write the 'total_distance' field.
				if(allColumns || columns.hasChild(Result.JSON_KEY_TOTAL_DISTANCE)) {
					generator
						.writeNumberField(
							Result.JSON_KEY_TOTAL_DISTANCE,
							result.totalDistance);
				}
				
				// Write the 'type' field.
				if(allColumns || columns.hasChild(Result.JSON_KEY_TYPE)) {
					generator
						.writeStringField(
							Result.JSON_KEY_TYPE,
							result.type);
				}
				
				// Write the 'uri' field.
				if(allColumns || columns.hasChild(Result.JSON_KEY_URI)) {
					generator
						.writeStringField(
							Result.JSON_KEY_URI,
							result.uri);
				}
				
				// End the data object.
				generator.writeEndObject();
				
				// End the overall object.
				generator.writeEndObject();
			}
		}
	}
	
	/**
	 * A factory class for generating {@link RunKeeperApi} objects.
	 *
	 * @author John Jenkins
	 */
	public static enum RunKeeperApiFactory {
		PROFILE
			(ProfileApi.PATH, ProfileApi.class),
		FITNESS_ACTIVITIES 
			(FitnessActivitiesApi.PATH, FitnessActivitiesApi.class);
		
		private final String apiString;
		private final Class<? extends RunKeeperApi> apiClass;
		
		/**
		 * The mapping of path strings to their corresponding objects.
		 */
		private static final Map<String, RunKeeperApiFactory> FACTORY = 
			new HashMap<String, RunKeeperApiFactory>();
		static {
			// Populate the 'FACTORY' object.
			RunKeeperApiFactory[] paths = values();
			for(int i = 0; i < paths.length; i++) {
				RunKeeperApiFactory path = paths[i];
				FACTORY.put(path.apiString, path);
			}
		}
		
		/**
		 * Default constructor made private to prevent instantiation.
		 */
		private RunKeeperApiFactory(
				final String apiString, 
				final Class<? extends RunKeeperApi> apiClass) {
			
			if(apiString == null) {
				throw new IllegalArgumentException("The API string is null.");
			}
			if(apiString.trim().length() == 0) {
				throw new IllegalArgumentException(
					"The API string is all whitespace.");
			}
			if(apiClass == null) {
				throw new IllegalArgumentException(
					"The API class is null.");
			}
			
			this.apiString = apiString;
			this.apiClass = apiClass;
		}
		
		/**
		 * Returns the API's string value.
		 * 
		 * @return The API's string value.
		 */
		public final String getApi() {
			return apiString;
		}
		
		/**
		 * Returns a new instance of the RunKeeperApi object specified by the
		 * 'api' parameter.
		 * 
		 * @param api The string to use to lookup a RunKeeperApi object.
		 * 
		 * @return The RunKeeperApi object that corresponds to the 'api'
		 * 		   parameter.
		 * 
		 * @throws DomainException The API was unknown or there was an error
		 * 						   generating an instance of it.
		 */
		public static final RunKeeperApi getApi(
				final String api)
				throws DomainException {
			
			if(FACTORY.containsKey(api)) {
				try {
					return FACTORY.get(api).apiClass.newInstance();
				}
				catch(InstantiationException e) {
					throw new DomainException(
						"The Class for the path cannot be instantiated: " +
							api,
						e);
				}
				catch(IllegalAccessException e) {
					throw new DomainException(
						"The Class for the path does not contain a no-argument constructor: " +
							api,
						e);
				}
				catch(SecurityException e) {
					throw new DomainException(
						"The security manager prevented instantiation for the path: " +
							api,
						e);
				}
				catch(ExceptionInInitializerError e) {
					throw new DomainException(
						"The constructor for the path threw an exception: " +
							api,
						e);
				}
			}
			
			throw new DomainException("The path is unknown: " + api);
		}
	}
	private final RunKeeperApi api;
	
	/**
	 * Creates a payload ID that contains a type representing the type of 
	 * information desired, e.g. "profile", "sleep", "weight", etc..
	 * 
	 * @param api The type of information desired.
	 * 
	 * @throws DomainException The API value is null or whitespace.
	 */
	public RunKeeperPayloadId(
		final String[] runKeeperPayloadParts)
		throws ValidationException {
		
		// Verify that there are exactly three parts.
		if(runKeeperPayloadParts.length != 3) {
			throw
				new ValidationException(
					ErrorCode.OMH_INVALID_PAYLOAD_ID,
					"The RunKeeper payload ID must be exactly three parts.");
		}
		
		try {
			this.api = RunKeeperApiFactory.getApi(runKeeperPayloadParts[2]);
		}
		catch(DomainException e) {
			throw
				new ValidationException(
					ErrorCode.OMH_INVALID_PAYLOAD_ID,
					"The path for the RunKeeper payload ID is unknown.");
		}
	}
	
	/**
	 * The type of information desired, e.g. "profile", "sleep", "weight", 
	 * etc..
	 * 
	 * @return The API name.
	 */
	public String getApi() {
		return api.getPath();
	}
	
	/**
	 * Outputs the Concordia definition for the current API.
	 * 
	 * @param generator
	 *        The JsonGenerator to write the Concordia definition to.
	 * 
	 * @throws IOException
	 *         There was an error writing to the generator.
	 */
	public void toConcordia(
		final JsonGenerator generator)
		throws IOException {
		
		api.toConcordia(generator);
	}

	/**
	 * Returns an OMH read request for Run Keeper.
	 * 
	 * @return An OmhReadMoodMapRequest object.
	 */
	@Override
	public UserRequest generateReadRequest(
			final HttpServletRequest httpRequest,
			final Map<String, String[]> parameters,
			final Boolean hashPassword,
			final TokenLocation tokenLocation,
			final boolean callClientRequester,
			final long version,
			final String owner,
			final DateTime startDate,
			final DateTime endDate,
			final long numToSkip,
			final long numToReturn) 
			throws DomainException {
		
		try {
			return
				new OmhReadRunKeeperRequest(
					httpRequest,
					parameters,
					hashPassword,
					tokenLocation,
					callClientRequester,
					api);
		}
		catch(IOException e) {
			throw new DomainException(
				"There was an error reading the HTTP request.",
				e);
		}
		catch(InvalidRequestException e) {
			throw new DomainException(
				"Error parsing the parameters.",
				e);
		}
		catch(IllegalArgumentException e) {
			throw new DomainException(
				"One of the parameters was invalid.",
				e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.ohmage.domain.PayloadId#generateWriteRequest(javax.servlet.http.HttpServletRequest, java.util.Map, java.lang.Boolean, org.ohmage.request.UserRequest.TokenLocation, boolean, long, java.lang.String)
	 */
	@Override
	public UserRequest generateWriteRequest(
		final HttpServletRequest httpRequest,
		final Map<String, String[]> parameters,
		final Boolean hashPassword,
		final TokenLocation tokenLocation,
		final boolean callClientRequester,
		final long version,
		final String data)
		throws DomainException {

		throw new DomainException(
			ErrorCode.OMH_INVALID_PAYLOAD_ID,
			"Cannot write to this payload ID.");
	}
}