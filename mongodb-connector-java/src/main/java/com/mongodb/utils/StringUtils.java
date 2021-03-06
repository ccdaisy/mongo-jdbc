package com.mongodb.utils;
/**
 * 
 */


/**
 * @author SunNing
 * 
 * @since Aug 18, 2010
 */
/**
 * @author ccdaisy
 *
 */
public class StringUtils {

	/**
	 * 
	 * @param indentSize
	 * @param content
	 * @return
	 */
	public static String asLine(int indentSize, String... content) {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < indentSize; i++) {
			sb.append(" ");
		}

		if (content != null) {
			for (String c : content)
				sb.append(c);
		}
		sb.append("\n");

		return sb.toString();
	}

	/**
	 * 
	 * @param content
	 * @return
	 */
	public static String quote(String content) {
		StringBuffer sb = new StringBuffer();
		sb.append("\"");
		sb.append(content);
		sb.append("\"");

		return sb.toString();
	}

	/**
	 * 
	 * @param c
	 * @return
	 */
	public static String capticalize(String c) {
		StringBuffer sb = new StringBuffer();
		sb.append(Character.toUpperCase(c.charAt(0)));
		sb.append(c.substring(1));

		return sb.toString();
	}

	/**
	 * 
	 * @param origin
	 * @param spliter
	 * @return
	 */
	public static String[] splitAtLast(String origin, String spliter) {
		int pos = origin.lastIndexOf(spliter);

		String[] results = new String[2];
		results[0] = origin.substring(0, pos);
		results[1] = origin.substring(pos + 1);

		return results;
	}
	
	/**
	 * @param origin
	 * @param spilter
	 * @return
	 */
	public static String[] splitAtFirst(String origin, String spilter){
		int pos = origin.indexOf(spilter);
		
		String[] results = new String[2];
		results[0] = origin.substring(0, pos);
		results[1] = origin.substring(pos + 1);
		
		return results;
	}

	/**
	 * Transform a string from "java.util.List&lt;String&gt;" to
	 * "java.util.List"
	 * 
	 * @param str
	 * @return
	 */
	public static String removeGenericQuote(String str) {
		if (str == null) {
			return null;
		}
		
		// is a generic type
		if (str.charAt(str.length()-1) == '>') {
			int lt = str.lastIndexOf('<'); 
			return str.substring(0, lt);
		}
		
		return str;
	}
	
	/**
	 * 
	 * @param arrayType
	 * @return
	 */
	public static String arrayTypeToZeroLengthArray(String arrayType){
		int o= arrayType.indexOf("[]");
		if (o < 0){
			throw new IllegalStateException("Not a array type");
		} 
		
		StringBuffer sb = new StringBuffer();
		sb.append(arrayType.substring(0, o)).append("[0]");
		
		return sb.toString();
	}
}
