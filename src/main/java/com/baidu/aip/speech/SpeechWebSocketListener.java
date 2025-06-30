package com.baidu.aip.speech;

import com.baidu.aip.util.Util;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONException;
import org.json.JSONObject;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * websocket 监听器
 *
 * @author ningy
 */
public class SpeechWebSocketListener extends WebSocketListener {

    private static java.util.logging.Logger logger = Logger.getLogger("SpeechWebSocketListener");

    private AtomicBoolean isClosed;

    private Flowable<byte[]> flowable;

    private SpeechStat stat;

    private FlowableEmitter<Map<String, Object>> emitter;

    private AipSpeech aipSpeech;

    private Disposable speechSendTask;

    private final Long socketId = System.currentTimeMillis();

    private final Integer devPid;

    public SpeechWebSocketListener(AipSpeech apiSpeech, boolean isEn, Flowable<byte[]> flowable, FlowableEmitter<Map<String, Object>> emitter) {
        this.aipSpeech = apiSpeech;
        this.devPid = isEn? 1737 : 1537;
        isClosed = new AtomicBoolean(false); // 是否
        stat = new SpeechStat(); //一些统计数据

        this.flowable = flowable;
        this.emitter = emitter;
        stat.updateBeforeConnectTime();
    }

    @Override
    public void onOpen(@NotNull WebSocket webSocket, @NotNull Response response) {
        super.onOpen(webSocket, response);
        stat.updateOnOpenTime();

        // 这里千万别阻塞，包括这个类其它回调
//        this.speechSendTask.setWebSocket(webSocket);
//        executorService.submit(this.speechSendTask);
        // 2.1 发送发送开始参数帧
        sendStartFrame(webSocket);
        // 2.2 发送心跳帧
//        sendHeartBeat(webSocket);
        // 2.3 实时发送音频数据帧
        sendAudioFrames(webSocket);
    }

    /**
     * 2.1 发送发送开始参数帧
     *
     * @param webSocket WebSocket 类
     * @throws JSONException Json解析错误
     */
    protected void sendStartFrame(WebSocket webSocket) throws JSONException {
        JSONObject params = new JSONObject();

        params.put("appid", Long.parseLong(this.aipSpeech.getAppId()));
        params.put("appkey", this.aipSpeech.getAipKey());

        params.put("dev_pid", devPid);
        params.put("cuid", "self_defined_server_id_like_mac_address");

        params.put("format", "pcm");
        params.put("sample", 16000);

        JSONObject json = new JSONObject();
        json.put("type", "START");
        json.put("data", params);

        logger.info("asr websocket: " + socketId + " send start FRAME:" + json.toString());
        webSocket.send(json.toString());
    }

    /**
     * STEP 2.2 发送心跳帧
     *
     * @param webSocket WebSocket 类
     */
    private void sendHeartBeat(WebSocket webSocket) {
        JSONObject json = new JSONObject();
        json.put("type", "HEARTBEAT");
        speechSendTask = Flux.interval(Duration.ofSeconds(5))
                .subscribe(e -> {
                    logger.info("socketId: " + socketId + " send heartbeat FRAME:" + json.toString());
                    webSocket.send(json.toString());
                });
    }

    /**
     * STEP 2.3 实时发送音频数据帧
     *
     * @param webSocket WebSocket 类
     */
    protected void sendAudioFrames(WebSocket webSocket) {
        ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
        this.flowable.doOnNext(e -> {
                    int bytesPerFrame = Util.BYTES_PER_FRAME;  // 一个帧 160ms的音频数据
                    byte[] buffer = new byte[bytesPerFrame];
                    int readSize = -1;
                    int totalSize = 0;
                    buffer = e;
                    readSize = buffer.length;
                    if (readSize > 0) {
                        // 发送二进制，积累到160ms，即5120个字节再发送
                        outputBuffer.write(buffer, 0, readSize);
                        if (outputBuffer.size() >= bytesPerFrame) {
                            byte[] buf = outputBuffer.toByteArray();
                            sendBytes(webSocket, buf);
//                            logger.log(Level.INFO, "send DATA FRAME: " + Arrays.toString(buffer) + " | total size: " + totalSize);
                            outputBuffer.reset();
                            outputBuffer.write(buf, bytesPerFrame, buf.length - bytesPerFrame);
                            totalSize += bytesPerFrame;
                            logger.finer("should wait to send next DATA Frame: " + Util.FRAME_MS
                                    + "ms | send binary bytes size :" + bytesPerFrame + " | total size: " + totalSize);
                        }
                    }
                })
                .doOnComplete(()-> System.out.println("complete"))
                .doOnError(e -> System.out.println("error: " + e.getMessage()))
                .doOnCancel(() -> System.out.println("cancel"))
                .doOnTerminate(()-> System.out.println("terminate"))
                .subscribe();
        // 通知consumer发送数据帧
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("event", "SESSION_START");
        resultMap.put("type", "EVENT");
        emitter.onNext(resultMap);
    }

    protected int sendBytes(WebSocket webSocket, byte[] buffer) {
        return sendBytes(webSocket, buffer, buffer.length);
    }

    /**
     * 发送二进制帧
     *
     * @param webSocket WebSocket类
     * @param buffer 二进制
     * @param size
     * @return
     */
    protected int sendBytes(WebSocket webSocket, byte[] buffer, int size) {
//        logger.info("" + socketId + " send DATA FRAME: " + Util.bytesToTime(size) + "ms | size: " + size);
        if (size > 0) {
            ByteString bytesToSend = ByteString.of(buffer, 0, size);
            webSocket.send(bytesToSend);
            return Util.bytesToTime(size);
        } else if (size == 0) {
            logger.severe("read size is 0");
            return 100;
        }
        return 0;
    }

    @Override
    public void onMessage(@NotNull WebSocket webSocket, @NotNull String text) {
        super.onMessage(webSocket, text);
        // 这里千万别阻塞，包括这个类其它回调
        SpeechResult result;
        try {
            // 将json解析为SpeechRtResult类
            result = new SpeechResult(text);
            logger.log(Level.INFO, "receive text: " + text + ",  json: " + result);
            if (!emitter.isCancelled()) {
                if (result.getResult() != null && !result.isHeartBeat()) {
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put("result", result.getResult());
                    resultMap.put("type", result.getType());
                    resultMap.put("resp", result);
                    emitter.onNext(resultMap);
                }
            }
        } catch (JSONException e) {
            logger.log(Level.SEVERE, "receive json parse error: " + e.getMessage() + ":" + text, e);
            e.printStackTrace();
            return;
        }
        if (result.isHeartBeat()) {
            logger.finest("receive heartbeat: " + text.trim());
        }
        if (result.isFin()) {
            stat.addResult(result);
        }
    }

    @Override
    public void onClosed(@NotNull WebSocket webSocket, int code, @NotNull String reason) {
        super.onClosed(webSocket, code, reason);
        // 这里千万别阻塞，包括这个类其它回调
        logger.info("baidu asr websocket: " + socketId + " closed: " + code + " | " + reason);
        logger.info(stat.toReportString());
        setClosed(code, reason);
    }

    @Override
    public void onClosing(@NotNull WebSocket webSocket, int code, @NotNull String reason) {
        super.onClosing(webSocket, code, reason);
        // 这里千万别阻塞，包括这个类其它回调
        logger.info("websocket event closing :" + code + " | " + reason);
        webSocket.close(1000, "");
    }

    @Override
    public void onFailure(@NotNull WebSocket webSocket, @NotNull Throwable t, @Nullable Response response) {
        super.onFailure(webSocket, t, response);
        // 这里千万别阻塞，包括这个类其它回调
        logger.log(Level.SEVERE, "websocket " + socketId + " failure :" + t.getMessage(), t);
        setClosed(500, t.getMessage());
    }

    private void setClosed(Integer code, String reason) {
        isClosed.set(true);
        // 关闭心跳任务
//        speechSendTask.dispose();
        this.emitter.onError(new RuntimeException("baidu asr websocket " + socketId + " closed: " + code + " | " + reason));
    }


    @Override
    public void onMessage(@NotNull WebSocket webSocket, @NotNull ByteString bytes) {
        super.onMessage(webSocket, bytes);
        logger.severe("receive binary unexpected: " + bytes.size());
        // never happen
    }

    public boolean isClosed() {
        return isClosed.get();
    }
}
