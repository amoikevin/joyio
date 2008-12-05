/*
 * DataSet.java
 *
 * Created on 2007年12月11日, 下午8:54
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package org.joy.io;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.db.LockMode;
import java.util.Comparator;

/**
 * 数据库的游标类
 * @author 海
 */
public interface Reader extends Comparable<Reader> {
	
    void close() throws DBException;

    Object getKey();

    Object getValue();

    void delete() throws DBException;

    void put(Object key, Object value) throws DBException;

    boolean next() throws DBException;

    boolean next(LockMode lockMode) throws DBException;

    EntryBinding getKeyBinding();

    EntryBinding getValueBinding();

    int count() throws DBException;

    DataSet orderBy(Comparator c, int count) throws DBException;
}
