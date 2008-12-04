/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.joy.io.persist;

import java.lang.reflect.Field;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 *
 * @author Lamfeeling
 */
@Entity
public class Singer {

    @PrimaryKey
    String name;

    public Singer(String name) {
        this.name = name;
    }

    public Singer() {
    }

    public String getName() {
        return name;
    }
    public static void main(String []args){
//    	MP3 m = new MP3("a","b");
//		//从e中解析出主键，并读出
//		for (Field f : m.getClass().getDeclaredFields()) {
//			if (f.isAnnotationPresent(PrimaryKey.class)) {
//				try {
//					System.out.println(f.get(m));
//				} catch (Exception e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
//			}
//		}
    }
}
