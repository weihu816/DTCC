package cn.ict.dtcc.config;

import java.util.Map;

public class Sharding_micro {
	public static Member getShardMember(Map<Integer,Member[]> members, String table, String key) {
//		if (key.equals("ROOT") ) {
//			return members.get(0)[0];
//		} else {
//			return members.get(0)[Integer.parseInt(key.substring(5,6)) % 2];
//		}
		switch (table) {
		case "table1":
			return members.get(0)[0];
		case "table2":
			 return members.get(0)[1];
		case "table3":
			 return members.get(0)[2];
		default:
			break;
		}
		return null;
	}
}
