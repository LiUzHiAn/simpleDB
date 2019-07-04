package simpledb.file;

import simpledb.server.SimpleDB;

/**
 * @program: simpleDB
 * @description: 测试file模块中的类
 * @author: LiuZhian
 * @create: 2019-07-04 15:02
 **/
public class FileTest {


    public static void main(String[] args) {
        SimpleDB.init("simpleDBTest");

        Page p1 = new Page();
        Block blk = new Block("junk", 6);
        p1.read(blk);

        int n = p1.getInt(102);
        System.out.println(n);
        p1.setInt(102, n + 1);
        p1.write(blk);
        System.out.println(p1.getInt(102));

        Page p2 = new Page();
        p2.setString(20, "hello");
        blk = p2.append("junk");
        Page p3 = new Page();
        p3.read(blk);
        String s = p3.getString(20);
        System.out.println(s);
    }
}
