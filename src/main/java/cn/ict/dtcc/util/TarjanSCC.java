package cn.ict.dtcc.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import cn.ict.rcc.messaging.Node;

public class TarjanSCC {

    private Map<String, Boolean> marked;
    private Map<String, Integer> id;            // id[v] = id of strong component containing v
    private Map<String, Integer> low;           // low[v] = low number of v
    private Stack<String> stack;
    private int pre;
    private int count;
    private String s;
    private List<String> result = new ArrayList<String>();

    public TarjanSCC(String s, Map<String, List<Node>> G) {
    	this.s = s;
        marked = new HashMap<String, Boolean>();
        id = new HashMap<String, Integer>();
        low = new HashMap<String, Integer>();
        for (Map.Entry<String, List<Node>> e : G.entrySet()) {
        	marked.put(e.getKey(), false);
        	id.put(e.getKey(), -1);
        	low.put(e.getKey(), -1);
        }
        stack = new Stack<String>();
        dfs(G, s);
    }

    private void dfs(Map<String, List<Node>> G, String v) { 
        marked.put(v, true);
        low.put(v, pre++);
        int min = low.get(v);
        stack.push(v);

        for (Node n : G.get(v)) {
        	String w = n.getId();
            if (!marked.get(w)) {
            	dfs(G, w);
            }
            if (low.get(w) < min) { min = low.get(w); }
        }
        if (min < low.get(v)) { low.put(v, min); return; }
        String w;
        
        boolean flag = false;
        if (v == s) { flag = true; }
        do {
            w = stack.pop();
            if (flag) { result.add(w); }
            id.put(w, count);
            low.put(w, G.size());
        } while (w != v);
        count++;
    }

    public List<String> get() {
    	System.out.println(id);
    	return result;
    }
    
    public static void main(String[] args) {
    	Map<String, List<Node>> G = new HashMap<String, List<Node>>();
    	List<Node> s = new ArrayList<Node>();
    	s.add(new Node("2", false));
    	G.put("1", s);
    	s = new ArrayList<Node>();
    	s.add(new Node("3", false));
    	s.add(new Node("4", true));
    	G.put("2", s);
    	s = new ArrayList<Node>();
    	s.add(new Node("1", false));
    	G.put("3", s);
    	s = new ArrayList<Node>();
    	s.add(new Node("3", false));
    	G.put("4", s);
    	TarjanSCC scc = new TarjanSCC("1", G);
    	List<String> result = scc.get();
    	System.out.println(result);
    }
}
