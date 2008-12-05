/*
 * WebBaseConfiguration.java
 *
 * Created on 2007年8月2日, 下午11:40
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.joy.io.config;

import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseType;


/**
 * 标准的数据库配置类，提供默认的数据库配置，数据库类型Type BTree,允许创建文件
 * @author 海
 */
public class DefaultDBConfig extends DatabaseConfig{
    
    /** Creates a new instance of DefaultDBConfig */
    public DefaultDBConfig() {
        setType(DatabaseType.BTREE);
        setAllowCreate(true);
    }
    
}
