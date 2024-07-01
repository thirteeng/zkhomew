/*
 *@Type NormalStore.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:07
 * @version
 */
package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import controller.SocketServerHandler;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.jar.JarEntry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class NormalStore implements Store {

    public static final String TABLE = ".table";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";

    private static final int MAX_LOG_SIZE = 30; // 轮换阈值，用来进行轮换

    private ExecutorService compressionExecutor = Executors.newSingleThreadExecutor(); // 单线程池用于文件压缩

    /**
     * 内存表，类似缓存
     */
    private TreeMap<String, Command> memTable;

    /**
     * hash索引，存的是数据长度和偏移量
     */
    private HashMap<String, CommandPos> index;

    /**
     * 数据目录
     */
    private final String dataDir;

    /**
     * 读写锁，支持多线程，并发安全写入
     */
    private final ReadWriteLock indexLock;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile writerReader;

    /**
     * 持久化阈值
     */
    private final int storeThreshold;


    /**
     * 构造函数，初始化数据目录、内存表和索引
     *
     * @param dataDir 数据目录
     */
    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<String, Command>();
        this.index = new HashMap<>();
        this.storeThreshold = 2; // 阀值

        //创建数据目录
        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER, logFormat, "NormalStore", "dataDir isn't exist,creating...");
            file.mkdirs();
        }

        //回放
        this.redo();

        //加载已有的数据索引
        this.reloadIndex();

        // 注册关闭钩子，在程序退出时将内存表中的数据写入磁盘！！
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                flushMemTableToDisk();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }

    /**
     * 生成日志文件的路径
     *
     * @return 日志文件路径
     */
    public String genFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }


    /**
     * 加载已有的索引数据
     * 从日志文件中读取命令并重建内存中的索引
     */
    public void reloadIndex() {
        try {
            RandomAccessFile file = new RandomAccessFile(this.genFilePath(), RW_MODE);
            long len = file.length();
            long start = 0;
            file.seek(start);

            // 读取日志文件中的每个命令并重建索引
            while (start < len) {
                int cmdLen = file.readInt(); // 读取命令长度
                byte[] bytes = new byte[cmdLen];
                file.read(bytes); // 读取命令内容
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    index.put(command.getKey(), cmdPos); // 更新索引
                }
                start += cmdLen;
            }
            file.seek(file.length());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 设置键值对
     *
     * @param key   键
     * @param value 值
     */
    @Override
    public void set(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);

            // 加锁，确保线程安全
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘
            // 保存到memTable内存表
            memTable.put(key, command);//
            // TODO://判断是否需要将内存表中的值写回table
            if (memTable.size() >= storeThreshold) {
                flushMemTableToDisk();
            }

            rotateLogFile(); // 检查并进行文件轮换
            // 添加索引
            CommandPos cmdPos = new CommandPos(0, 0);//0占位，实际写更新
            index.put(key, cmdPos);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * 根据键获取值
     *
     * @param key 键
     * @return 值
     */
    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();//上锁
            // 先从内存表获取
            if (memTable.containsKey(key)) {
                Command command = memTable.get(key);
                if (command instanceof SetCommand) {
                    return ((SetCommand) command).getValue();
                } else if (command instanceof RmCommand) {
                    return null;//删除命令，删了
                }
            }
            // 从索引中获取信息
            CommandPos cmdPos = index.get(key);
            if (cmdPos == null) {
                return null;
            }

            //从磁盘来读取命令
            byte[] commandBytes = RandomAccessFileUtil.readByIndex(this.genFilePath(), cmdPos.getPos(), cmdPos.getLen());
            JSONObject value = JSONObject.parseObject(new String(commandBytes));
            Command cmd = CommandUtil.jsonToCommand(value);
            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
            }
            if (cmd instanceof RmCommand) {
                return null;//删除命令则null
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();//去锁
        }
        return null;
    }


    /**
     * 删除键值对
     *
     * @param key 键
     */
    @Override
    public void rm(String key) {
        try {
            RmCommand command = new RmCommand(key);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘
            // 保存到memTable
            memTable.put(key, command);
            // TODO://判断是否需要将内存表中的值写回table
            if (memTable.size() >= storeThreshold) {
                flushMemTableToDisk();
            }


            // 添加索引
            CommandPos cmdPos = new CommandPos(0, 0);
            index.put(key, cmdPos);
            rotateLogFile();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }


    /**
     * 将内存表中的数据写入磁盘
     *
     * @throws IOException
     */
    private void flushMemTableToDisk() throws IOException {
//        try (RandomAccessFile file = new RandomAccessFile(this.genFilePath(), RW_MODE)) {
        for (String key : memTable.keySet()) {
            Command command = memTable.get(key);

            String commandStr = command.toString();

            RandomAccessFileUtil.writeInt(this.genFilePath(), commandStr.length());
            byte[] commandBytes = commandStr.getBytes(StandardCharsets.UTF_8);

            //写入命令内容，获取写入位置
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);

            //更新索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
        }
        memTable.clear();//内存表清空
//        }
    }

    /**
     * 检查日志文件大小并进行轮换111
     *
     * @throws IOException
     */
    private void rotateLogFile() throws IOException {
        File logFile = new File(this.genFilePath());
        if (logFile.length() >= MAX_LOG_SIZE) {
            Path genFile = Paths.get(genFilePath());
            String rotatedFilePath = this.genFilePath() + "." + System.currentTimeMillis(); // 生成新的文件名
            Files.copy(genFile, Paths.get(rotatedFilePath));
            compressLogFile(rotatedFilePath); // 压缩旧的日志文件
            Files.write(genFile, new byte[0]);
        }
    }

    /**
     * 压缩日志文件
     *
     * @param filePath 要压缩的文件路径
     */
    private void compressLogFile(String filePath) {
        compressionExecutor.submit(() -> {
            try (FileInputStream fis = new FileInputStream(filePath);
                 FileOutputStream fos = new FileOutputStream(filePath + ".gz");
                 GZIPOutputStream gzos = new GZIPOutputStream(fos)) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = fis.read(buffer)) > 0) {
                    gzos.write(buffer, 0, len); // 将文件内容写入压缩流
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    Files.deleteIfExists(Paths.get(filePath));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    //回放功能的实现
    public void redo() {
        List<Path> paths = null;
        try {
            paths = Files.list(Paths.get(dataDir)).filter(p -> p.getFileName().toString().endsWith("gz")).sorted().collect(Collectors.toList());//将文件按照序号排列
            paths.add(Paths.get(genFilePath()));//把最后一个没压缩的添加到list里面
            LoggerUtil.debug(LOGGER, "redo list: {}", paths);
            for (Path path : paths) {
                try {
                    RandomAccessFile file;
                    if(path.getFileName().toString().equals(NAME + TABLE)){
                        file = new RandomAccessFile(path.getFileName().toString(), RW_MODE);
                    }else {
                        file = readGzipToMemory(path.toString());
                        if(file == null){
                            continue;
                        }
                    }

                    long len = file.length();
                    long start = 0;
                    file.seek(start);

                    // 读取日志文件中的每个命令并重建索引
                    while (start < len) {
                        int cmdLen = file.readInt(); // 读取命令长度
                        byte[] bytes = new byte[cmdLen];
                        file.read(bytes); // 读取命令内容
                        JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                        Command command = CommandUtil.jsonToCommand(value);
                        memTable.put(command.getKey(), command);
                        LoggerUtil.debug(LOGGER, "redo: read command {}", command);

                        start += 4;
                        CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                        index.put(command.getKey(), cmdPos); // 更新索引
                        LoggerUtil.debug(LOGGER, "redo: load index {} {}", command.getKey(), cmdPos);
                        start += cmdLen;
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            LoggerUtil.debug(LOGGER, "redo success! mem: {}", memTable);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    public RandomAccessFile readGzipToMemory(String gzipFile) {
        byte[] buffer = new byte[1024];
        try {
            FileInputStream fileIn = new FileInputStream(gzipFile);
            if (fileIn.available() == 0) {
                return null;
            }
            GZIPInputStream gZIPInputStream = new GZIPInputStream(fileIn);

            File tempFile = File.createTempFile(gzipFile, ".tmp");
            FileOutputStream fos = new FileOutputStream(tempFile);

            int bytes_read;

            while ((bytes_read = gZIPInputStream.read(buffer)) > 0) {
                fos.write(buffer, 0, bytes_read);
            }

            gZIPInputStream.close();
            fos.close();
            return new RandomAccessFile(tempFile, RW_MODE);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }



    /**
     * 关闭资源，在程序退出时调用
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        flushMemTableToDisk();
    }
}
