package com.datastax.demo.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaTeardown extends RunCQLFile
{
	private static final Logger logger = LoggerFactory.getLogger(SchemaSetup.class);
	private static final String CQL_FILE_PATH_DEFAULT = "src/main/resources/cql/drop_schema.cql";

	SchemaTeardown(String cqlPath)
	{
		super(cqlPath);
	}

	public static void main(String args[])
	{
		try
		{
			String cqlPath = getOptionalArgument(args, CQL_FILE_PATH_DEFAULT);
			SchemaTeardown teardown = new SchemaTeardown(cqlPath);
			teardown.execute();
			teardown.shutdown();
		}
		catch (Exception e)
		{
			logger.error("Fatal error tearing down schema.", e);
		}
	}
}
