package com.hmdp.utils;

/**
 * @author Ivan Fioon
 * @create 2023/3/31 - 22:06
 */
public interface ILock {
    /**
     * 尝试获取锁
     * @param timeoutSec
     * @return true 获取锁成功 false 获取锁失败
     */
    boolean tryLock(long timeoutSec);

    /**
     * 释放锁
     */
    void unlock();
}
