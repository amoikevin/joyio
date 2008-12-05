/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.joy.io.persist;

import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joy.io.persist.BoundGenerator.Bound;

/**
 * 
 * @author Lamfeeling
 */
public class SQLParser {

	private String command;
	private Class<?> entityClass;
	private Hashtable<String, Bound> bounds = new Hashtable<String, Bound>();

	public Hashtable<String, Bound> getBounds() {
		return bounds;
	}

	public Class getEntityClass() {
		return entityClass;
	}

	public SQLParser(String command) throws ClassNotFoundException {
		this.command = command;
		parse();
	}

	private void resolveWhereClause(String orgin) {
		orgin = " " + orgin;
		Pattern p = Pattern.compile("\\s*\\w+\\s*" + "(>=|<=|=|>|<)"
				+ "(\\s*\\d*\\.*\\d*\\w\\s*|\\s*'.*'\\s*)");
		Matcher m = p.matcher(orgin);
		while (m.find()) {
			// 获取表达式
			String expression = m.group().trim();
			String t[] = expression.split("(>=|<=|=|>|<)");
			assert(t.length == 2);
			//去除空格
			t[0] = t[0].trim();
			t[1] = t[1].trim();
			
			String op = expression.substring(t[0].length(), expression
					.indexOf(t[1])).trim();

			// 获取上下界信息
			if (t[1].startsWith("'") && t[1].endsWith("'")) {
				String realVal = t[1].substring(1, t[1].length() - 1);
				bounds.put(t[0],
						new Bound<String>(true, realVal, true, realVal));
				System.out.println(new Bound<String>(true, realVal, true,
						realVal));
			} else {
				BoundGenerator bg = new BoundGenerator(bounds.get(t[0]), op,
						t[1]);
				bounds.put(t[0], bg.getBounds());
				System.out.println(bg.getBounds());
			}
		}
	}

	public static void main(String[] args) throws ClassNotFoundException {
		// resolveWhereClause("a>=12 and b<=21 and b>2 and a<100");
		new SQLParser("select from org.joy.io.persist.MP3 where t='a'");
	}

	private void parse() throws ClassNotFoundException {
		String[] terms = command.split(" ");

		for (int i = 0; i < terms.length; i++) {
			String term = terms[i];
			if (term.equals("from")) {
				entityClass = Class.forName(terms[i + 1]);
			}
		}
		// 获取Where后的字符串
		String where = command.substring(command.indexOf("where")
				+ "where".length(), command.length());
		resolveWhereClause(where);
	}
}
