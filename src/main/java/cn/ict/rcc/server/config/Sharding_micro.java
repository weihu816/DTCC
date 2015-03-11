package cn.ict.rcc.server.config;

import java.util.Map;

import cn.ict.rcc.Member;

public class Sharding_micro {
	public static Member getShardMember(Map<Integer,Member[]> members, String table, String key) {
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