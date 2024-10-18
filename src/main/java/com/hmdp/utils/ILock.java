package com.hmdp.utils;

/**
 * @author yanghe
 * @create 2024-09-08-17:16
 * @description: TODO
 */
public interface ILock {

    /**
     *
     * @param timeoutSec 锁持有的超时时间，过期自动释放
     * @return:boolean  成功true
     * @author: yanang
     * @date: 2024/9/8 17:18
     * description
     **/

    boolean tryLock(long timeoutSec);
    void unlock();


}
