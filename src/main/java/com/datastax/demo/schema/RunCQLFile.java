package com.datastax.demo.schema;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public abstract class RunCQLFile
{

	private static Logger logger = LoggerFactory.getLogger(RunCQLFile.class);

	private static final String CONTACT_POINTS_PROP_NAME = "contactPoints";
	private static final String CONTACT_POINTS_SEP = ",";
	private static final String CONTACT_POINTS_DEFAULT = "127.0.0.1";
	private static final String CQL_LINE_SEP = ";";

	private Cluster cluster;
	private Session session;
	private String cqlPath;

	RunCQLFile(String cqlPath)
	{
		logger.info("Running CQL in file: " + cqlPath);
		this.cqlPath = cqlPath;
	}

	void execute() throws IOException
	{
		String contactPointsStr = PropertyHelper.getProperty(CONTACT_POINTS_PROP_NAME, CONTACT_POINTS_DEFAULT);
		cluster = Cluster.builder().addContactPoints(contactPointsStr.split(CONTACT_POINTS_SEP)).build();
		session = cluster.connect();
		executeFile();
	}

	void shutdown()
	{
		session.close();
		cluster.close();
	}

	private void executeFile() throws IOException
	{
		File cqlFile = new File(cqlPath);
		String readFileIntoString = FileUtils.readFileToString(cqlFile);
		String[] commands = readFileIntoString.split(CQL_LINE_SEP);

		for (String command : commands)
		{
			String cql = command.trim();

			if (cql.isEmpty())
			{
				continue;
			}

			boolean ignoreFailure = cql.toLowerCase().startsWith("drop");

			if (ignoreFailure)
			{
				executeLineIgnoreFailure(cql);
			}
			else
			{
				executeLine(cql);
			}
		}
	}

	private void executeLine(String cql)
	{
		logger.info("Executing: " + cql);
		session.execute(cql);
	}

	private void executeLineIgnoreFailure(String cql)
	{
		try
		{
			executeLine(cql);
		}
		catch (InvalidQueryException e)
		{
			logger.info("Ignoring exception.", e);
		}
	}

	static String getOptionalArgument(String[] args, String defaultValue)
	{
		String cqlPath;

		if (args.length > 0 && StringUtils.isNotBlank(args[0]))
		{
			cqlPath = args[0];
		}
		else
		{
			cqlPath = defaultValue;
		}

		return cqlPath;
	}
}
