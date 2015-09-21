package com.datastax.demo.schema;

public class SchemaTeardown extends RunCQLFile {

	SchemaTeardown(String cqlFile) {
		super(cqlFile);
	}

	public static void main(String args[]){
		
		SchemaTeardown setup = new SchemaTeardown("cql/drop_schema.cql");
		setup.internalSetup();
		setup.shutdown();
	}
}