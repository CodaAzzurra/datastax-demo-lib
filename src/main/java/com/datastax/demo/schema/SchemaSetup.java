package com.datastax.demo.schema;

public class SchemaSetup extends RunCQLFile {

	private static final String CQL_FILE_PATH_DEFAULT = "cql/create_schema.cql";

	SchemaSetup(String cqlPath) {
		super(cqlPath);
	}

	public static void main(String args[]) {

		String cqlPath = getOptionalArgument(args, CQL_FILE_PATH_DEFAULT);
		SchemaSetup setup = new SchemaSetup(cqlPath);
		setup.internalSetup();
		setup.shutdown();
	}
}
