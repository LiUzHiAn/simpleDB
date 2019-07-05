package simpledb.buffer;

import simpledb.file.Page;

/**
 * 新建一个块中内容的接口，在simpleDB中，有数据块（data block）、索引块（index block）
 * @program: simpleDB
 * @description:
 * @author: LiuZhian
 * @create: 2019-07-05 17:17
 **/
public interface PageFormatter {
    /**
     * 初始化一个页缓冲数组中的内容，该内容会然后被append到一个文件的新块内
     * {@link Buffer#assignToNew}.
     * @param p a buffer page
     */
    public void format(Page p);
}
