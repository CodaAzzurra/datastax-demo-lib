package com.datastax.demo.utils;

public class PropertyHelper
{
	public static String getProperty(String name, String defaultValue)
	{
		String value = System.getProperty(name);
		return value == null ? defaultValue : value;
	}
}
