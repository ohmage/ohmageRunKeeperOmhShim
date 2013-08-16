package org.ohmage.jee.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.ohmage.cache.OmhThirdPartyRegistration;
import org.ohmage.domain.RunKeeperPayloadId;
import org.ohmage.domain.RunKeeperPayloadIdBuilder;

/**
 * <p>
 * Registers the RunKeeper payload IDs.
 * </p>
 *
 * @author John Jenkins
 */
public class RunKeeperRegistrationListener implements ServletContextListener {
	/**
	 * Default constructor.
	 */
	public RunKeeperRegistrationListener() {
		// Do nothing.
	}

	/*
	 * (non-Javadoc)
	 * @see javax.servlet.ServletContextListener#contextInitialized(javax.servlet.ServletContextEvent)
	 */
	@Override
	public void contextInitialized(final ServletContextEvent event) {
		// Register the RunKeeper payload ID.
		OmhThirdPartyRegistration
			.registerDomain(
				RunKeeperPayloadId.DOMAIN_ID,
				new RunKeeperPayloadIdBuilder());
	}

	/*
	 * (non-Javadoc)
	 * @see javax.servlet.ServletContextListener#contextDestroyed(javax.servlet.ServletContextEvent)
	 */
	@Override
	public void contextDestroyed(final ServletContextEvent event) {
		// Do nothing.
	}
}