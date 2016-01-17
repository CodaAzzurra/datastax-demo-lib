package com.datastax.demo.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaSetup extends RunCQLFile
{
	private static final Logger logger = LoggerFactory.getLogger(SchemaSetup.class);
	private static final String CQL_FILE_PATH_DEFAULT = "src/main/resources/cql/create_schema.cql";

	SchemaSetup(String cqlPath)
	{
		super(cqlPath);
	}

	public static void main(String args[])
	{
		try
		{
			String cqlPath = getOptionalArgument(args, CQL_FILE_PATH_DEFAULT);
			SchemaSetup setup = new SchemaSetup(cqlPath);
			setup.execute();
			setup.shutdown();
		}
		catch (Exception e)
		{
			logger.error("Fatal error setting up schema.", e);
		}
	}
}
