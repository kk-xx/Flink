package checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Info {
    public static void main(String[] args) throws IOException {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置状态后端
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/flinkck"));
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/flinkCk"));

        //3.配置CK
        //3.1开启ck并设置间隔,头与头间隔,与模式
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //3.2设置同时有多少个ck任务,
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);
        //3.3设置ck超时时间
        env.getCheckpointConfig().setCheckpointTimeout(1000L);
        //3.4设置CK重试次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        //3.4两个CK之间最小的时间间隔,头与尾间隔,(这个会与同时又多少个ck任务冲突,这个设置就不会有同时的ck)
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        //3.5如果存在最近的savepoint,是否采用savapoint恢复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(false);

        //4.重启策略
        //4.1固定延迟重启策略,尝试重启3次,每次重启时间间隔5秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
        //4.2失败率重启策略,50秒内重启3次,每次重启时间间隔为5秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.seconds(50),Time.seconds(5)));
    }
}
