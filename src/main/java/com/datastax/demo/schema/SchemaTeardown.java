package com.datastax.demo.schema;

public class SchemaTeardown extends RunCQLFile {

	private static final String CQL_FILE_PATH_DEFAULT = "cql/drop_schema.cql";

	SchemaTeardown(String cqlPath) {
		super(cqlPath);
	}

	public static void main(String args[]) {

		String cqlPath = getOptionalArgument(args, CQL_FILE_PATH_DEFAULT);
		SchemaTeardown teardown = new SchemaTeardown(cqlPath);
		teardown.execute();
		teardown.shutdown();
	}
}
