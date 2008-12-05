/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.joy.io;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.db.CursorConfig;

/**
 * 一个简单的大容量的Hash表，键和值的类型都是string,提供类似于栈的pop操作，
 * 同时取出下一个Key和Value
 * @author 海
 */
public class TaskSet extends DataSet {

    public TaskSet() throws DBException {
        super(new StringBinding(), new StringBinding());
    }

    public String[] pop() throws DBException {
        CursorConfig cursorConfig = new CursorConfig();
        cursorConfig.setWriteCursor(true);
        Reader reader = getReader(cursorConfig);
        if (reader.next()) {
            String key = (String) reader.getKey();
            String value = (String) reader.getValue();
            reader.delete();
            reader.close();
            return new String[]{key, value};
        }
        reader.close();
        return null;
    }
}
