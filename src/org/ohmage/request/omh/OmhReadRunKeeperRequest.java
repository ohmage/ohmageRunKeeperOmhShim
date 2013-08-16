package org.ohmage.request.omh;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.ohmage.annotator.Annotator.ErrorCode;
import org.ohmage.domain.RunKeeperPayloadId;
import org.ohmage.domain.RunKeeperPayloadId.RunKeeperApi;
import org.ohmage.exception.DomainException;
import org.ohmage.exception.InvalidRequestException;
import org.ohmage.exception.ServiceException;
import org.ohmage.request.UserRequest;
import org.ohmage.request.observer.StreamReadRequest.ColumnNode;
import org.ohmage.service.OmhServices;

/**
 * This is an Open mHealth-compliant read for RunKeeper data.
 *
 * @author John Jenkins
 */
public class OmhReadRunKeeperRequest
	extends UserRequest
	implements OmhReadServicer, OmhReadResponder {
	
	private static final Logger LOGGER = 
		Logger.getLogger(OmhReadRunKeeperRequest.class);

	private final RunKeeperApi api;
	
	/**
	 * Creates a request to read a RunKeeper API.
	 * 
	 * @param httpRequest The HTTP request.
	 * 
	 * @param parameters The parameters already decoded from the HTTP request.
	 * 
	 * @param hashPassword Whether or not to hash the user's password on
	 * 					   authentication. If null, username and password are
	 * 					   not allowed for this API.
	 * 
	 * @param tokenLocation Where to search for the user's token. If null, a
	 * 						token is not allowed for this API.
	 * 
	 * @param callClientRequester Refers to the "client" parameter as the
	 * 							  "requester".
	 * 
	 * @param owner The user whose data is being requested. If null, the 
	 * 				requester is requesting data about themselves.
	 * 
	 * @param startDate Limits the results to only those on or after this date.
	 * 
	 * @param endDate Limits the results to only those on or before this date.
	 * 
	 * @param columns Limits the data output based on the given columns.
	 * 
	 * @param numToSkip The number of responses to skip. Responses are in 
	 * 					reverse-chronological order.
	 * 
	 * @param numToReturn The number of responses to return after the required
	 * 					  responses have been skipped.
	 * 
	 * @param api The path to the API to be called.
	 * 
	 * @throws IOException There was an error reading from the request.
	 * 
	 * @throws InvalidRequestException Thrown if the parameters cannot be 
	 * 								   parsed. This is only applicable in the
	 * 								   event of the HTTP parameters being 
	 * 								   parsed.
	 */
	public OmhReadRunKeeperRequest(
		final HttpServletRequest httpRequest,
		final Map<String, String[]> parameters,
		final Boolean hashPassword,
		final TokenLocation tokenLocation,
		final boolean callClientRequester,
		final RunKeeperApi api)
		throws IOException, InvalidRequestException {
		
		super(
			httpRequest,
			hashPassword,
			tokenLocation,
			parameters,
			callClientRequester);
		
		if(! isFailed()) {
			LOGGER.info("Creating an OMH read request for RunKeeper.");
		}
		
		if(api == null) {
			DomainException nullApi = new DomainException("The API is null.");
			nullApi.failRequest(this);
			nullApi.logException(LOGGER);
		}
		this.api = api;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.ohmage.request.Request#service()
	 */
	@Override
	public void service() {
		DomainException e =
			new DomainException(
				ErrorCode.SYSTEM_GENERAL_ERROR,
				"This method should never be called.");
		e.failRequest(this);
		e.logException(LOGGER);
	}

	/*
	 * (non-Javadoc)
	 * @see org.ohmage.request.Request#service()
	 */
	@Override
	public void service(
		final String owner,
		final DateTime startDate,
		final DateTime endDate,
		final long numToSkip,
		final long numToReturn) {
		
		LOGGER.info("Servicing an OMH read request for RunKeeper.");
		
		try {
			// Get the authentication information from the database.
			LOGGER
				.info("Getting the authentication credentials for RunKeeper.");
			Map<String, String> runKeeperCredentials =
				OmhServices
					.instance().getCredentials(RunKeeperPayloadId.DOMAIN_ID);
			
			// Retrieve the bearer's credentials for this user.
			String bearer = runKeeperCredentials.get("bearer_" + owner);
			if(bearer == null) {
				// If the user is not linked, we treat it as if they have no
				// data.
				LOGGER
					.info(
						"The user's account is not linked, so we are returning no data.");
				return;
			}
			
			// Get the data and massage it into a form we like.
			try {
				LOGGER
					.info(
						"Calling the RunKeeper API: " +
							api.getUri().toString());
				api.service(
					bearer, 
					startDate, 
					endDate, 
					numToSkip, 
					numToReturn);
			}
			catch(DomainException e) {
				throw new ServiceException("Could not retrieve the data.", e);
			}
		}
		catch(ServiceException e) {
			e.failRequest(this);
			e.logException(LOGGER);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.ohmage.request.omh.OmhReadResponder#getNumDataPoints()
	 */
	@Override
	public long getNumDataPoints() {
		return api.getNumDataPoints();
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
		
		LOGGER.info("Responding to an OMH read request for RunKeeper data.");
		
		// We call through to the API to respond.
		api.respond(generator, columns);
	}

	/*
	 * (non-Javadoc)
	 * @see org.ohmage.request.Request#respond(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	public void respond(
			final HttpServletRequest httpRequest,
			final HttpServletResponse httpResponse) {
		
		if(isFailed()) {
			super.respond(httpRequest, httpResponse, (JSONObject) null);
		}
		else {
			throw new UnsupportedOperationException(
				"HTTP requests are invalid for this request.");
		}
	}
}