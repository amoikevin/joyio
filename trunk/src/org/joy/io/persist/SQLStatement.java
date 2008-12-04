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
 * SQL语句类，负责解析SQL语句，以提供相关查询信息
 * @author Lamfeeling
 */
public class SQLStatement {

    private String command;
    private Class<?> entityClass;
    private Hashtable<String, Bound> bounds = new Hashtable<String, Bound>();

    /**
     * 从语句中解析出所Where语句的限制范围和索引字段名
     * @return 所有的Where语句的限制范围和索引字段名
     */
    public Hashtable<String, Bound> getBounds() {
        return bounds;
    }

    /**
     * 从语句中解析出需要查询的Entity表单类
     * @return 需要查询的Entity表单类
     */
    public Class getEntityClass() {
        return entityClass;
    }

    /**
     * 构造一个查询器
     * @param command
     * @throws java.lang.ClassNotFoundException
     */
    public SQLStatement(String command) throws ClassNotFoundException {
        this.command = command;
        parse();
    }

    private void resolveWhereClause(String orgin) {
        orgin = " " + orgin;
        //按照AND分段
        for (String part : orgin.split(" and ")) {
            Pattern p = Pattern.compile("\\s*\\w+\\s*" + "(>=|<=|=|>|<)" +
                    "(\\s*\\d*\\.*\\d*\\w\\s*|\\s*'.*'\\s*)");
            Matcher m = p.matcher(part);
            while (m.find()) {
                // 获取表达式
                String expression = m.group().trim();
                String t[] = expression.split("(>=|<=|=|>|<)");
                String op = expression.substring(t[0].length(), expression.indexOf(t[1]));
                assert t.length == 2;
                t[0] = t[0].trim();
                t[1] = t[1].trim();
                op = op.trim();

                //获取上下界信息
                if (t[1].startsWith("'") && t[1].endsWith("'")) {
                    String realVal = t[1].substring(1, t[1].length() - 1);
                    bounds.put(t[0], new Bound(true, realVal, true, realVal));
                    System.out.println(new Bound(true, realVal, true, realVal));
                } else {
                    BoundGenerator bg = new BoundGenerator(bounds.get(t[0]), op, t[1]);
                    bounds.put(t[0], bg.getBounds());
                    System.out.println(bg.getBounds());
                }
            }
        }
    }

    public static void main(String[] args) throws ClassNotFoundException {
        //resolveWhereClause("a>=12 and b<=21 and b>2 and a<100");
        new SQLStatement("select from org.joy.io.persist.MP3 where t='a'");
    }

    private void parse() throws ClassNotFoundException {
        String[] terms = command.split(" ");

        for (int i = 0; i < terms.length; i++) {
            String term = terms[i];
            if (term.equals("from")) {
                entityClass = Class.forName(terms[i + 1]);
            }
        }
        //获取Where后的字符串
        String where = command.substring(command.indexOf("where") + "where".length(), command.length());
        resolveWhereClause(where);
    }
}
