package org.ohmage.domain;

import org.ohmage.exception.ValidationException;

/**
 * <p>
 * This class manages converting RunKeeper payload IDs into
 * {@link RunKeeperPayloadId} objects.
 * </p>
 *
 * @author John Jenkins
 */
public class RunKeeperPayloadIdBuilder implements PayloadIdBuilder {
	/**
	 * Default constructor.
	 */
	public RunKeeperPayloadIdBuilder() {}

	/*
	 * (non-Javadoc)
	 * @see org.ohmage.domain.PayloadIdBuilder#build(java.lang.String[])
	 */
	@Override
	public PayloadId build(
		final String[] payloadIdParts)
		throws ValidationException {

		return new RunKeeperPayloadId(payloadIdParts);
	}
}