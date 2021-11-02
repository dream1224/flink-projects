package common.conn;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtils {
    // 单例模式创建线程池
    // 声明线程池
    private static ThreadPoolExecutor threadPoolExecutor;

    // 私有化构造方法
    private ThreadPoolUtils() {

    }

    /**
     * 单例模式确保ThreadPoolExecutor唯一
     * 创建ThreadPoolExecutor
     *
     * @return
     */
    public static ThreadPoolExecutor getThreadPoolExecutor() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtils.class) {
                if (threadPoolExecutor == null) {
                    TimeUnit unit;
                    BlockingQueue workQueue;
                    threadPoolExecutor = new ThreadPoolExecutor(4, 20, 100L, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }
}
