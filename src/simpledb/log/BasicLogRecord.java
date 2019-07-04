package simpledb.log;

import simpledb.file.Page;


import static simpledb.file.Page.INT_SIZE;
import static simpledb.file.Page.STR_SIZE;

/**
 * @program: simpleDB
 * @description: 基本的日志记录对象
 * 改类的对象本身不知道日志的具体信息，只提供了nextInt()和nextString()方法
 * 调用该对象方法的客户端应该负责，知道哪个位置存的是什么类型的数据
 * @author: LiuZhian
 * @create: 2019-07-04 17:23
 **/
public class BasicLogRecord {
    private Page pg;   // 保存log记录的页
    private int pos;   // 页中该log记录的位置指针

    public BasicLogRecord(Page pg, int pos) {
        this.pg = pg;
        this.pos = pos;
    }

    public int nextInt() {
        int result = pg.getInt(pos);
        pos += INT_SIZE;  // 指针移动
        return result;
    }

    public String nextString() {
        String result=pg.getString(pos);
        pos += STR_SIZE(result.length());  // 指针移动
        return result;
    }
}
