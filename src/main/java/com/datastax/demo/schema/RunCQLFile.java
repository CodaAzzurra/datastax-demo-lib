package com.datastax.demo.schema;

import com.datastax.demo.utils.FileUtils;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RunCQLFile {

	public static final String CONTACT_POINTS_PROPNAME = "contactPoints";
	private static Logger logger = LoggerFactory.getLogger(RunCQLFile.class);
	private static final String CONTACT_POINTS_DEFAULT = "127.0.0.1";

	private Cluster cluster;
	private Session session;
	private String cqlPath;

	RunCQLFile(String cqlPath) {

		logger.info("Running CQL in file: " + cqlPath);
		this.cqlPath = cqlPath;
	}

	void internalSetup() {

		String contactPointsStr = PropertyHelper.getProperty(CONTACT_POINTS_PROPNAME, CONTACT_POINTS_DEFAULT);
		cluster = Cluster.builder().addContactPoints(contactPointsStr.split(",")).build();
		session = cluster.connect();

		executeFile();
	}

	private void executeFile() {
		String readFileIntoString = FileUtils.readFileIntoString(cqlPath);

		String[] commands = readFileIntoString.split(";");

		for (String command : commands) {

			String cql = command.trim();

			if (cql.isEmpty()) {
				continue;
			}

			boolean ignoreFailure = cql.toLowerCase().startsWith("drop");
			executeLine(cql, ignoreFailure);
		}
	}

	private void executeLine(String cql, boolean ignoreFailure) {

		try {
			logger.info("Executing: " + cql);
			session.execute(cql);
		}
		catch (InvalidQueryException e) {

			if (ignoreFailure) {
				logger.info("Ignoring exception: ", e);
			}
			else {
				throw e;
			}
		}
	}

	void sleep(int i) {
		try {
			Thread.sleep(i);
		}
		catch (Exception e) {
		}
	}

	void shutdown() {
		session.close();
		cluster.close();
	}

	static String getOptionalArgument(String[] args, String defaultValue) {

		String cqlPath;

		if (args.length > 0 && StringUtils.isNotBlank(args[0])) {
			cqlPath = args[0];
		}
		else {
			cqlPath = defaultValue;
		}

		return cqlPath;
	}
}
