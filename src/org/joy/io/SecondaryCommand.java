/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.joy.io;

import com.sleepycat.db.CursorConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.SecondaryCursor;

/**
 * 用来操作二级数据库的类，search方法和Command类相同，只不过对象是二级数据库（索引数据库）
 * @author 海
 */
public class SecondaryCommand extends Command {

    private SecondaryConnection secConn = (SecondaryConnection) super.conn;

    public SecondaryCommand(SecondaryConnection conn) {
        super(conn);
    }

    public Reader search(Object key, boolean writeCursor) throws DBException {
        try {
            CursorConfig cursorConfig = new CursorConfig();
            if (writeCursor) {
                cursorConfig.setWriteCursor(true);
            }
            SecondaryCursor c = (SecondaryCursor) secConn.getDb().openCursor(null, cursorConfig);
            DatabaseEntry keyE = new DatabaseEntry();
            secConn.getKeyBinding().objectToEntry(key, keyE);

            Reader r = new SSRReader(c, secConn.getReaderPool(), keyE,
                    secConn.getKeyBinding(),
                    secConn.getPrimaryKeyBinding(),
                    secConn.getValueBinding());
            return r;
        } catch (DatabaseException ex) {
            ex.printStackTrace();
            throw new DBException();
        }
    }

    public Reader search(Object key) throws DBException {
        return search(key, false);
    }

    public Reader search(Object key, Object pKey, boolean writeCursor) throws DBException {
        try {
            CursorConfig cursorConfig = new CursorConfig();
            if (writeCursor) {
                cursorConfig.setWriteCursor(true);
            }
            SecondaryCursor c = (SecondaryCursor) secConn.getDb().openCursor(null, cursorConfig);
            DatabaseEntry keyE = new DatabaseEntry(), pKeyE = new DatabaseEntry();
            secConn.getKeyBinding().objectToEntry(key, keyE);
            secConn.getPrimaryKeyBinding().objectToEntry(pKey, pKeyE);

            Reader r = new SSRReader(c, secConn.getReaderPool(), keyE, pKeyE,
                    secConn.getKeyBinding(), secConn.getPrimaryKeyBinding(), secConn.getValueBinding());
            return r;
        } catch (DatabaseException ex) {
            ex.printStackTrace();
            throw new DBException();
        }
    }
}
