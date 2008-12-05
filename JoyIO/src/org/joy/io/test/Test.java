package org.joy.io.test;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.joy.io.Platform;
import org.joy.io.DBException;
import org.joy.io.persist.EntityCommand;
import org.joy.io.persist.EntityConnection;
import org.joy.io.persist.EntityReader;
import org.joy.io.persist.Store;

public class Test {

    public static void main(String[] args) throws DBException, ClassNotFoundException {
        Platform p = new Platform("mp3");
        EntityConnection s = p.open(Singer.class);
        EntityCommand<String, Singer> c1 = new EntityCommand<String, Singer>(s);
        c1.setEntity(new Singer("周杰伦"));
        EntityConnection ec = p.open(MP3.class);
        System.out.println("写入测试...");
        try {
            final EntityCommand<String, MP3> c = new EntityCommand<String, MP3>(ec);
            Thread t = new Thread(new Runnable() {

                public void run() {
                    for (int i = 0; i < 10000; i++) {
                        try {
                            c.setEntity(new MP3("ab" + i, "周杰伦",12L,i));
                            EntityReader<MP3> mp3 = c.search("singer", "周杰伦");
                            //System.out.println("---");
                            mp3.next();
                            if (mp3.next()) {
                                //System.out.println(mp3.getEntity().getName());
                            }
                            mp3.close();
                        } catch (DBException ex) {
                            Logger.getLogger(Store.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
            });
            Thread t2 = new Thread(new Runnable() {

                public void run() {
                    for (int i = 10000; i < 20000; i++) {
                        try {
                            EntityReader<MP3> mp3 = c.search("ab0");
                            //System.out.println("---");
                            mp3.next();
                            if (mp3.next()) {
                                //System.out.println(mp3.getEntity().getName());
                            }
                            mp3.close();
                        } catch (DBException ex) {
                            Logger.getLogger(Store.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
            });
            t.start();
            t2.start();
            t.join();
            MP3 a = c.getEntity("ab23");
            System.out.println(a.getSinger());
            t2.join();
            System.out.println("测试结束");
        } catch (InterruptedException ex) {
            Logger.getLogger(Store.class.getName()).log(Level.SEVERE, null, ex);
        }
        p.syncAndClose();
    }
}
