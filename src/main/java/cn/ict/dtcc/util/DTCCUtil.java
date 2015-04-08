package cn.ict.dtcc.util;

import java.util.ArrayList;
import java.util.List;

public class DTCCUtil {

	public static String buildString(String... args) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < args.length; i++) {
			stringBuffer.append(args[i]);
		}
		return stringBuffer.toString();
	}
	
	public static String buildKey(List<String> args) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < args.size() - 1; i++) {
			stringBuffer.append(args.get(i));
			stringBuffer.append("_");
		}
		stringBuffer.append(args.get(args.size() - 1));
		return stringBuffer.toString();
	}

	public static String buildKey(Object... args) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < args.length - 1; i++) {
			stringBuffer.append(args[i]);
			stringBuffer.append("_");
		}
		stringBuffer.append(args[args.length - 1]);
		return stringBuffer.toString();
	}
	
	public static List<String> buildColumns(Object... args) {
		List<String> columns = new ArrayList<String>();
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof String) {
				columns.add((String) args[i]);
			} else {
				columns.add(String.valueOf(args[i]));
			}
		}
		return columns;
	}
}
