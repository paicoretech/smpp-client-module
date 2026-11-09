package com.paicbd.module.smpp;

import com.paicbd.module.utils.AppProperties;
import com.paicbd.module.utils.Utils;
import com.paicbd.smsc.cdr.CdrProcessor;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.MessagePart;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.exception.NoAvailableSessionException;
import com.paicbd.smsc.kafka.KafkaConsumerConstants;
import com.paicbd.smsc.kafka.KafkaConsumerCustomImpl;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import com.paicbd.smsc.kafka.KafkaConsumerHandler;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.kafka.KafkaUtils;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.scylla.ScyllaTablesConstants;
import com.paicbd.smsc.utils.ChargingUtils;
import com.paicbd.smsc.utils.multipart.MultipartPartStatuses;
import com.paicbd.smsc.utils.multipart.MultipartRetryMode;
import com.paicbd.smsc.utils.multipart.MultipartRetryPolicy;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.EncodingUtils;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.utils.SmppUtils;
import com.paicbd.smsc.utils.UtilsEnum;
import com.paicbd.smsc.utils.Watcher;
import com.paicbd.smsc.ws.SocketSession;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.InvalidResponseException;
import org.jsmpp.PDUException;
import org.jsmpp.bean.DataCoding;
import org.jsmpp.bean.ESMClass;
import org.jsmpp.bean.OptionalParameter;
import org.jsmpp.bean.RegisteredDelivery;
import org.jsmpp.extra.NegativeResponseException;
import org.jsmpp.extra.ResponseTimeoutException;
import org.jsmpp.session.BindParameter;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.session.Session;
import org.jsmpp.session.SubmitSmResult;
import org.jsmpp.util.DeliveryReceiptState;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import com.paicbd.smsc.utils.RedisManager;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.paicbd.module.utils.Constants.CDR_COMMENT;
import static com.paicbd.module.utils.Constants.SKIPPED_PART_COMMENT;
import static com.paicbd.module.utils.Constants.PARAM_UPDATE_SESSIONS;
import static com.paicbd.module.utils.Constants.PARAM_UPDATE_STATUS;
import static com.paicbd.module.utils.Constants.STOPPED;
import static com.paicbd.smsc.utils.GeneralSmscConstants.MSG_REFERENCE_TYPE;
import static com.paicbd.smsc.utils.GeneralSmscConstants.MSG_REFERENCE_TYPE_8BIT;

/**
 * @author <a href="mailto:enmanuelcalero61@gmail.com"> Enmanuel Calero </a>
 * @author <a href="mailto:ndiazobed@gmail.com"> Obed Navarrete </a>
 */
@Slf4j
public class SmppConnectionManager extends KafkaConsumerHandler {
    private final SecureRandom secureRandom = new SecureRandom();
    private final CdrProcessor cdrProcessor = new CdrProcessor();

    @Getter
    private final List<SMPPSession> sessions = new CopyOnWriteArrayList<>();
    private final AtomicInteger requestCounterTotal = new AtomicInteger(0);

    private final AppProperties appProperties;
    private final SocketSession socketSession;
    private final ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap;
    private final MessageReceiverListenerImpl messageReceiverListener;
    @Getter
    private final SessionStateListenerImpl sessionStateListener;
    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaConsumerFactory kafkaConsumerFactory;
    private ExecutorService sendingExecutor;
    private ScheduledExecutorService schedulerRetryBind;
    private Watcher submitSmWatcher;


    @Getter
    private Gateway gateway;
    private KafkaConsumerCustomImpl kafkaConsumerHigh;
    private KafkaConsumerCustomImpl kafkaConsumerMedium;
    private KafkaConsumerCustomImpl kafkaConsumerLow;
    private ScheduledExecutorService processorScheduler;
    private ScheduledFuture<?> processorTask;

    private volatile boolean stopped = true;

    @SuppressWarnings("java:S107")
    public SmppConnectionManager(
            RedisManager redisManager, Gateway gateway, SocketSession socketSession,
            ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap,
            AppProperties appProperties, ScyllaManager scyllaManager,
            KafkaTemplate<String, String> kafkaTemplate, KafkaConsumerFactory kafkaConsumerFactory) {
        this.socketSession = socketSession;
        this.sessionStateListener = new SessionStateListenerImpl(
                gateway, this.socketSession, redisManager, sessions, this::checkAndManageConsumerState,
                kafkaTemplate, appProperties.isBindNotificationEnabled()
        );
        this.gateway = gateway;
        this.errorCodeMappingConcurrentHashMap = errorCodeMappingConcurrentHashMap;
        this.appProperties = appProperties;
        this.scyllaManager = scyllaManager;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.messageReceiverListener = new MessageReceiverListenerImpl(this.gateway, this.kafkaTemplate, this.scyllaManager, this.appProperties);
        log.info("Gateway [{}] initialized with priority consumers. High: {}, Medium: {}, Low: {}",
                gateway.getSystemId(),
                gateway.getMessagesPerSecondHigh(),
                gateway.getMessagesPerSecondMedium(),
                gateway.getMessagesPerSecondLow());
    }

    private void initializeConsumers() {
        log.info("Initializing priority Kafka consumers for gateway {}", this.gateway.getSystemId());
        int networkId = this.gateway.getNetworkId();

        int mpsHigh = gateway.getMessagesPerSecondHigh();
        int mpsMedium = gateway.getMessagesPerSecondMedium();
        int mpsLow = gateway.getMessagesPerSecondLow();

        if (mpsHigh > 0) {
            String topicHigh = networkId + KafkaTopicsConstants.SMPP_HIGH_MESSAGE_TOPIC_SUFFIX;
            String groupIdHigh = KafkaConsumerConstants.SMPP_HIGH_MESSAGE_GROUP_ID_PREFIX + networkId;
            kafkaConsumerHigh = new KafkaConsumerCustomImpl(topicHigh,
                    groupIdHigh,
                    appProperties.getKafkaBootstrapServers(),
                    gateway.getMessagesPerSecond(),
                    kafkaConsumerFactory,
                    scyllaManager);
        }

        if (mpsMedium > 0) {
            String topicMedium = networkId + KafkaTopicsConstants.SMPP_MEDIUM_MESSAGE_TOPIC_SUFFIX;
            String groupIdMedium = KafkaConsumerConstants.SMPP_MEDIUM_MESSAGE_GROUP_ID_PREFIX + networkId;
            kafkaConsumerMedium = new KafkaConsumerCustomImpl(topicMedium,
                    groupIdMedium,
                    appProperties.getKafkaBootstrapServers(),
                    gateway.getMessagesPerSecond(),
                    kafkaConsumerFactory,
                    scyllaManager);
        }

        if (mpsLow > 0) {
            String topicLow = networkId + KafkaTopicsConstants.SMPP_LOW_MESSAGE_TOPIC_SUFFIX;
            String groupIdLow = KafkaConsumerConstants.SMPP_LOW_MESSAGE_GROUP_ID_PREFIX + networkId;
            kafkaConsumerLow = new KafkaConsumerCustomImpl(topicLow,
                    groupIdLow,
                    appProperties.getKafkaBootstrapServers(),
                    gateway.getMessagesPerSecond(),
                    kafkaConsumerFactory,
                    scyllaManager);
        }
    }

    public void updateGatewayInDeep(Gateway gateway) {
        log.debug("Updating Gateway with networkId {}, gateway {}", gateway.getNetworkId(), gateway);
        this.gateway = gateway;
        this.messageReceiverListener.setGateway(gateway);
        this.sessionStateListener.setGateway(gateway);
    }

    @Synchronized
    private void checkAndManageConsumerState() {
        boolean shouldBeRunning = !sessions.isEmpty();
        boolean isActuallyRunning = (processorTask != null && !processorTask.isCancelled());

        if (shouldBeRunning && !isActuallyRunning) {
            log.info("There are {} active sessions. Starting priority kafka consumers.", sessions.size());
            startConsumer();
        } else if (!shouldBeRunning && isActuallyRunning) {
            log.info("No active sessions. Stopping priority kafka consumers.");
            stopConsumer();
        }
    }


    private void startSenderScheduler() {
        if (Objects.isNull(this.processorScheduler)) {
            this.processorScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r);
                t.setName("Processor-Scheduler-NetworkId-" + this.gateway.getNetworkId());
                t.setDaemon(true);
                return t;
            });
        }

        if (Objects.isNull(this.sendingExecutor)) {
            this.sendingExecutor = Executors.newFixedThreadPool(
                    this.gateway.getPduProcessorDegree(),
                    r -> {
                        Thread t = new Thread(r);
                        t.setName("SenderPool-NetworkId-" + this.gateway.getNetworkId());
                        t.setDaemon(true);
                        return t;
                    }
            );
        }
    }

    private void stopSenderScheduler() {
        if (Objects.nonNull(this.sendingExecutor)) {
            sendingExecutor.shutdownNow();
            sendingExecutor = null;
        }
    }

    private void startConsumer() {
        this.startSenderScheduler();
        this.initializeConsumers();
        this.startConsumers();
        this.startProcessor();
        log.info("Started priority consumers and processor for gateway {}", gateway.getSystemId());
    }

    private void startConsumers() {
        if (Objects.nonNull(this.kafkaConsumerHigh)) {
            this.kafkaConsumerHigh.startConsumer();
        }
        if (Objects.nonNull(this.kafkaConsumerMedium)) {
            this.kafkaConsumerMedium.startConsumer();
        }
        if (Objects.nonNull(this.kafkaConsumerLow)) {
            this.kafkaConsumerLow.startConsumer();
        }
    }

    private void startProcessor() {
        processorTask = this.processorScheduler.scheduleAtFixedRate(() -> {
            try {
                if (sessions.isEmpty()) {
                    return;
                }
                this.processMessages();
            } catch (Exception e) {
                log.error("Error in message processor: {}", e.getMessage(), e);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private void processMessages() {
        int mpsHigh = gateway.getMessagesPerSecondHigh();
        int mpsMedium = gateway.getMessagesPerSecondMedium();
        int mpsLow = gateway.getMessagesPerSecondLow();

        var messages = this.fetchMessages(
                gateway.getMessagesPerSecond(),
                mpsHigh,
                mpsMedium,
                mpsLow,
                kafkaConsumerHigh,
                kafkaConsumerMedium,
                kafkaConsumerLow
        );

        if (messages.isEmpty()) {
            return;
        }
        this.sendSubmitSmList(messages);
    }

    private void stopConsumer() {
        this.stopProcessor();
        this.stopConsumers();
        this.stopSenderScheduler();
        log.info("Stopped priority consumers and processor for gateway {}", gateway.getSystemId());
    }

    private void stopConsumers() {
        if (Objects.nonNull(this.kafkaConsumerHigh)) {
            this.kafkaConsumerHigh.stopConsumer();
        }
        if (Objects.nonNull(this.kafkaConsumerMedium)) {
            this.kafkaConsumerMedium.stopConsumer();
        }
        if (Objects.nonNull(this.kafkaConsumerLow)) {
            this.kafkaConsumerLow.stopConsumer();
        }
        this.kafkaConsumerHigh = null;
        this.kafkaConsumerMedium = null;
        this.kafkaConsumerLow = null;
    }

    private void stopProcessor() {
        if (Objects.nonNull(this.processorTask)) {
            this.processorTask.cancel(true);
            this.processorTask = null;
        }
        if (Objects.nonNull(this.processorScheduler)) {
            this.processorScheduler.shutdownNow();
            this.processorScheduler = null;
        }
    }

    private int handleException(Exception exception, MessageEvent submitSmEvent) {
        String specificExceptionType = exception.getClass().getSimpleName();
        String message = "Failed on send submit_sm with id {} and messageId {} due to " + specificExceptionType;
        log.warn(message, submitSmEvent.getId(), submitSmEvent.getMessageId(), exception);
        return switch (exception) {
            case PDUException ignored -> ErrorCodes.PDU_EXCEPTION_ERROR;
            case ResponseTimeoutException ignored -> ErrorCodes.TIMEOUT_ERROR;
            case InvalidResponseException ignored -> ErrorCodes.INVALID_RESPONSE_EXCEPTION_ERROR;
            case NegativeResponseException negativeResponseException -> negativeResponseException.getCommandStatus();
            case IOException ignored -> ErrorCodes.IO_EXCEPTION_ERROR;
            case NoAvailableSessionException ignored -> ErrorCodes.SMPP_CONNECTION_UNAVAILABLE;
            default -> ErrorCodes.SYSTEM_ERROR;
        };
    }

    public void connect() {
        this.stopped = false;
        this.submitSmWatcher = new Watcher("SmppClient:OutboundSubmitSm-NetworkId-" + this.gateway.getNetworkId() + "-", this.requestCounterTotal, 1);
        if (Objects.isNull(this.schedulerRetryBind)) {
            this.schedulerRetryBind = Executors.newSingleThreadScheduledExecutor(
                    new CustomizableThreadFactory("RetryBind-Scheduler-NetworkId-" + this.gateway.getNetworkId() + "-")
            );
        }
        this.initSessions(this.gateway.getSessionsNumber() - this.sessions.size());
        this.retryConnection();
    }

    private SMPPSession createAndBindSmppSession(Gateway gateway) {
        try {
            SMPPSession smppSession = gateway.isTlsEnabled()
                    ? new SMPPSession(new TrustStoreSSLConnectionFactory(
                            appProperties.getTlsTruststorePath(),
                            appProperties.getTlsTruststorePassword()))
                    : new SMPPSession();
            smppSession.addSessionStateListener(this.sessionStateListener);
            smppSession.setMessageReceiverListener(this.messageReceiverListener);
            smppSession.setTransactionTimer(gateway.getPduTimeout());
            smppSession.setEnquireLinkTimer(this.gateway.getEnquireLinkPeriod());
            smppSession.setPduProcessorDegree(this.gateway.getPduProcessorDegree());
            smppSession.setQueueCapacity(500000);

            smppSession.connectAndBind(
                    gateway.getIp(), gateway.getPort(),
                    new BindParameter(
                            UtilsEnum.getBindType(gateway.getBindType()),
                            gateway.getSystemId(),
                            gateway.getPassword(),
                            gateway.getSystemType(),
                            UtilsEnum.getTypeOfNumber(gateway.getAddressTON()),
                            UtilsEnum.getNumberingPlanIndicator(gateway.getAddressNPI()),
                            gateway.getAddressRange(),
                            UtilsEnum.getInterfaceVersion(gateway.getInterfaceVersion())
                    ));
            if (smppSession.getSessionState().isBound()) {
                log.debug("SMPP session bound to {}:{} with systemId {}", gateway.getIp(), gateway.getPort(), gateway.getSystemId());
                return smppSession;
            }
        } catch (IOException e) {
            log.error("Error while connecting to Gateway {}", gateway.getSystemId());
        } catch (Exception e) {
            log.error("Failed to create SMPP session for Gateway {}: {}", gateway.getSystemId(), e.getMessage());
        }
        return null;
    }

    private void initSessions(int qq) {
        if ("stopped".equalsIgnoreCase(this.gateway.getStatus())
                || this.gateway.getEnabled() != 1
                || this.gateway.getSessionsNumber() < 1
                || this.sessions.size() >= this.gateway.getSessionsNumber()) {
            return;
        }

        log.info("Starting SMPP sessions for gateway with networkId {}, message per seconds High {}, Medium {}, Low {}, quantity {}",
                this.gateway.getNetworkId(), this.gateway.getMessagesPerSecondHigh(), this.gateway.getMessagesPerSecondMedium(), this.gateway.getMessagesPerSecondLow() ,qq);
        ThreadFactory virtualThreadFactory = Thread.ofVirtual()
                .name("Smpp-Bind-Worker-" + this.gateway.getNetworkId() + "-", 1)
                .factory();

        try (var executor = Executors.newThreadPerTaskExecutor(virtualThreadFactory)) {
            List<CompletableFuture<SMPPSession>> futures = IntStream.range(0, qq)
                    .mapToObj(i -> CompletableFuture.supplyAsync(
                            () -> this.createAndBindSmppSession(gateway),
                            executor)
                    )
                    .toList();

            List<SMPPSession> newSessions = futures.stream()
                    .map(CompletableFuture::join)
                    .filter(Objects::nonNull)
                    .toList();

            sessions.addAll(newSessions);
        }

        if (!sessions.isEmpty()) {
            this.stopped = false;
        }
        checkAndManageConsumerState();
    }

    public void stopConnection() {
        this.stopped = true;
        if (!this.sessions.isEmpty()) {
            var sessionsCopy = new ArrayList<>(this.sessions);
            sessions.clear();
            for (Session session : sessionsCopy) {
                session.unbindAndClose();
            }
        }
        gateway.setStatus(STOPPED);
        gateway.setSuccessSession(0);
        this.sessionStateListener.getSuccessSession().set(0);
        this.socketSession.sendStatus(String.valueOf(gateway.getNetworkId()), PARAM_UPDATE_SESSIONS, "0");
        this.sessionStateListener.updateOnRedis();
        this.socketSession.getStompSession().send("/app/handler-status", String.format("%s,%s,%s,%s", "gw", gateway.getNetworkId(), PARAM_UPDATE_STATUS, STOPPED));
        this.checkAndManageConsumerState();
        if (Objects.nonNull(this.submitSmWatcher))
            this.submitSmWatcher.stopWatching();

        if (Objects.nonNull(this.schedulerRetryBind)) {
            this.schedulerRetryBind.shutdownNow();
            this.schedulerRetryBind = null;
        }

        log.warn("Gateway with networkId {} has been stopped", gateway.getNetworkId());
    }

    private void retryConnection() {
        Runnable retryTask = () -> {
            if (this.stopped) {
                return;
            }

            if (gateway.getSuccessSession() < gateway.getSessionsNumber()) {
                this.initSessions(gateway.getSessionsNumber() - sessions.size());
            }
        };
        this.schedulerRetryBind.scheduleWithFixedDelay(retryTask, gateway.getBindRetryPeriod(), gateway.getBindRetryPeriod(), TimeUnit.MILLISECONDS);
    }

    private void addInCache(int registeredDelivery, MessageEvent submitSmEvent, SubmitSmResult submitSmResult) {
        if (Objects.equals(registeredDelivery, RequestDelivery.REQUEST_DLR.getValue())) {
            String messageIdResponse = cleanAndUpperString(submitSmResult.getMessageId());
            String submitSmServerId = "HTTP".equalsIgnoreCase(submitSmEvent.getOriginProtocol()) ?
                    submitSmEvent.getParentId() :
                    submitSmEvent.getMessageId();

            submitSmEvent.addCustomParam(GeneralSmscConstants.MESSAGE_PRIORITY, submitSmEvent.getSmscMessagePriority());
            log.debug("Requesting DLR for submit_sm with id {} and messageId {}", submitSmEvent.getId(), messageIdResponse);
            UtilsRecords.SubmitSmResponseEvent submitSmResponseEvent = new UtilsRecords.SubmitSmResponseEvent(
                    messageIdResponse,
                    System.currentTimeMillis() + "-" + System.nanoTime(),
                    submitSmEvent.getSystemId(),
                    messageIdResponse,
                    submitSmServerId, // could be submitSmEvent.getMessageId()
                    submitSmEvent.getOriginProtocol().toUpperCase(),
                    submitSmEvent.getOriginNetworkId(),
                    submitSmEvent.getOriginNetworkType(),
                    submitSmEvent.getMsgReferenceNumber(),
                    submitSmEvent.getTotalSegment(),
                    submitSmEvent.getSegmentSequence(),
                    submitSmEvent.getParentId(),
                    submitSmEvent.getDestNetworkId(),
                    submitSmEvent.isApplyForRefund(),
                    submitSmEvent.getCustomParams(),
                    submitSmEvent.isSplitForSmsc(),
                    submitSmEvent.getSource(),
                    submitSmEvent.getDestination(),
                    submitSmEvent.getSmscMessagePriority()
            );
            this.scyllaManager.insertIntoTable(ScyllaTablesConstants.SMPP_SUBMIT_SM_RESULT_TABLE,
                    submitSmResponseEvent.hashId(), submitSmResponseEvent.toString());
        }
    }

    private void sendToRetryProcess(MessageEvent submitSmEventToRetry, int errorCode) {
        log.warn("Starting retry process for submit_sm with id {} and error code {}", submitSmEventToRetry.getMessageId(), errorCode);
        if (errorContained(gateway.getNoRetryErrorCode(), errorCode)) {
            log.debug("Handling no retry for submit_sm with id {}", submitSmEventToRetry.getMessageId());
            this.handlerFailedMessage(submitSmEventToRetry, errorCode);
            return;
        }

        if (errorContained(gateway.getRetryAlternateDestinationErrorCode(), errorCode)) {
            log.debug("Handling retry alternate destination for submit_sm with id {}", submitSmEventToRetry.getMessageId());
            this.handleRetryAlternateDestination(submitSmEventToRetry);
            return;
        }

        log.debug("Handling auto retry for submit_sm with id {}", submitSmEventToRetry.getMessageId());
        this.handleAutoRetry(submitSmEventToRetry, errorCode);
    }

    private void handleRetryAlternateDestination(MessageEvent originalSubmitSmEventToRetry) {
        this.publishFailedCdr(originalSubmitSmEventToRetry);
        MessageEvent submitSmEventToRetry = Converter.deepCopy(originalSubmitSmEventToRetry, MessageEvent.class);
        this.prepareForRetry(submitSmEventToRetry);
        String kafkaTopic = KafkaUtils.getRoutingTopicPriority(submitSmEventToRetry.getSmscMessagePriority());
        log.debug("Retry for submit_sm with id {}. new networkId {}", submitSmEventToRetry.getMessageId(), submitSmEventToRetry.getDestNetworkId());
        this.kafkaTemplate.send(kafkaTopic, submitSmEventToRetry.toString());
    }

    private void handleAutoRetry(MessageEvent submitSmEventToRetry, int errorCode) {
        log.debug("AutoRetryErrorCodes defined {}, ReceivedErrorCode {}", gateway.getAutoRetryErrorCode(), errorCode);
        if (errorContained(gateway.getAutoRetryErrorCode(), errorCode)) {
            log.warn("AutoRetrying for submit_sm with id {}", submitSmEventToRetry.getMessageId());
            this.publishFailedCdr(submitSmEventToRetry);
            this.prepareForRetry(submitSmEventToRetry);
            String topicRetries = KafkaUtils.getRetriesTopicPriority(submitSmEventToRetry.getSmscMessagePriority());
            kafkaTemplate.send(topicRetries, submitSmEventToRetry.toString());
            log.debug("Successfully added to Kafka topic {} -> {}", topicRetries, submitSmEventToRetry);
            return;
        }

        log.debug("Failed to retry for submit_sm with id {}. The gateway {} doesn't have the error code {} for the retry process.", submitSmEventToRetry.getMessageId(), this.getGateway().getName(), errorCode);
        this.handlerFailedMessage(submitSmEventToRetry, errorCode);
    }

    private void prepareForRetry(MessageEvent submitSmEventToRetry) {
        submitSmEventToRetry.setErrorCode(null);
        submitSmEventToRetry.setRetry(true);
        submitSmEventToRetry.setRetryNumber(Objects.isNull(submitSmEventToRetry.getRetryNumber()) ? 1 : submitSmEventToRetry.getRetryNumber() + 1);
    }

    private boolean errorContained(String stringList, int errorCode) {
        return Arrays.stream(stringList.split(",")).toList().contains(String.valueOf(errorCode));
    }

    private void sendDeliverSmForFailedCases(MessageEvent submitSmEventToRetry, int errorCode) {
        if (submitSmEventToRetry.getRegisteredDelivery() == RequestDelivery.REQUEST_DLR.getValue()) {
            if (!submitSmEventToRetry.isFinalSegmentForSplitSmsc()) return;
            ErrorCodeMapping errorCodeMapping = getErrorCodeMapping(errorCode);
            MessageEvent deliverSmEvent = submitSmEventToRetry.createDeliveryReceiptMessage(errorCodeMapping, null);
            deliverSmEvent.addCustomParam(CDR_COMMENT,
                    ErrorCodes.getErrorDescription(UtilsEnum.Module.SMPP_CLIENT, errorCode, null));
            this.kafkaTemplate.send(KafkaTopicsConstants.PRE_DELIVER_TOPIC, deliverSmEvent.toString());
        }
    }

    private ErrorCodeMapping getErrorCodeMapping(Integer errorCode) {
        List<ErrorCodeMapping> errorCodeMappingList = errorCodeMappingConcurrentHashMap.get(String.valueOf(gateway.getMno()));
        return Optional.ofNullable(errorCodeMappingList)
                .flatMap(list -> list.stream().filter(errorCodeMapping -> errorCodeMapping.getErrorCode() == errorCode).findFirst())
                .orElseGet(() -> {
                    log.debug("No error code mapping found for mno {} with error {}. using status {}",
                            gateway.getMno(), errorCode, DeliveryReceiptState.UNDELIV);
                    ErrorCodeMapping defaultMapping = new ErrorCodeMapping();
                    defaultMapping.setErrorCode(errorCode);
                    defaultMapping.setDeliveryErrorCode(errorCode);
                    defaultMapping.setDeliveryStatus("UNDELIV");
                    return defaultMapping;
                });
    }

    private void sendSubmitSmList(List<MessageEvent> events) {
        for (MessageEvent submitSmEvent : events) {
            CompletableFuture.runAsync(() -> {
                try {
                    if (hasMessageParts(submitSmEvent)) {
                        sendMultipartMessage(submitSmEvent);
                    } else {
                        sendMessage(submitSmEvent);
                    }
                } catch (Exception e) {
                    log.error("Failed to send submit sm list", e);
                }
            }, sendingExecutor);
        }
    }

    private static boolean hasMessageParts(MessageEvent submitSmEvent) {
        return Objects.nonNull(submitSmEvent.getMessageParts()) && !submitSmEvent.getMessageParts().isEmpty();
    }

    private static boolean isSplitParent(MessageEvent submitSmEvent) {
        return hasMessageParts(submitSmEvent) && Objects.isNull(submitSmEvent.getSegmentSequence());
    }

    private void publishSkippedPartsCdr(MessageEvent submitSmEvent) {
        if (!hasMessageParts(submitSmEvent)) {
            return;
        }

        List<MessagePart> pendingParts =
                new ArrayList<>(MultipartRetryPolicy.orderBySegmentSequence(submitSmEvent.getPendingMessageParts()));
        if (pendingParts.isEmpty()) {
            return;
        }

        if (submitSmEvent.isSplitForSmsc() && !appProperties.isSmscGenerateCdrWhenSplitSmsc()) {
            log.warn("Skipping CDR for the unsent parts of parentId {} because the message was split by the SMSC",
                    submitSmEvent.getParentId());
            return;
        }

        MessagePart alreadyReportedPart = pendingParts.get(0);
        for (MessagePart messagePart : pendingParts) {
            if (Objects.equals(messagePart.getSegmentSequence(), alreadyReportedPart.getSegmentSequence())) {
                continue;
            }
            log.info("Creating CDR for skipped part {} of {} with parentId {} using error code {}",
                    messagePart.getSegmentSequence(), messagePart.getTotalSegment(),
                    submitSmEvent.getParentId(), ErrorCodes.PART_SKIPPED_PREVIOUS_FAILURE);
            MessageEvent skippedPart = buildEventForMessagePart(submitSmEvent, messagePart);
            skippedPart.setMessageParts(null);
            skippedPart.setErrorCode(ErrorCodes.PART_SKIPPED_PREVIOUS_FAILURE);
            this.cdrProcessor.publishCdr(skippedPart, "", UtilsEnum.Module.SMPP_CLIENT,
                    UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, SKIPPED_PART_COMMENT, kafkaTemplate);
        }
    }

    private void publishFailedCdr(MessageEvent submitSmEvent) {
        if (isSplitParent(submitSmEvent)) {
            log.debug("Skipping CDR for multipart parent {}, each attempted part reports its own",
                    submitSmEvent.getMessageId());
            return;
        }
        this.cdrProcessor.publishCdr(submitSmEvent, UtilsEnum.Module.SMPP_CLIENT,
                UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, kafkaTemplate);
    }

    private void sendMultipartMessage(MessageEvent submitSmEvent) {
        List<MessagePart> orderedParts =
                new ArrayList<>(MultipartRetryPolicy.orderBySegmentSequence(submitSmEvent.getMessageParts()));
        submitSmEvent.setMessageParts(orderedParts);

        MultipartRetryMode retryMode = MultipartRetryPolicy.resolve(submitSmEvent);
        boolean resumeFromFailedPart = MultipartRetryPolicy.shouldResumeFromFailedPart(submitSmEvent);

        if (!resumeFromFailedPart) {
            MultipartPartStatuses.markAllAsNotSent(submitSmEvent);
        }

        int startSegment = resumeFromFailedPart
                ? MultipartRetryPolicy.resolveResumeSegment(submitSmEvent, orderedParts)
                : MultipartRetryPolicy.FIRST_SEGMENT;

        if (resumeFromFailedPart) {
            MultipartPartStatuses.markAsNotSentFromSegment(submitSmEvent, startSegment);
        }

        if (submitSmEvent.isRetry()) {
            log.info("Retrying multipart SMPP message with parentId {} in mode {} from segment {}, sent flags {}",
                    submitSmEvent.getParentId(), retryMode, startSegment,
                    MultipartPartStatuses.sentFlags(submitSmEvent));
        }

        for (MessagePart messagePart : orderedParts) {
            Integer segmentSequence = messagePart.getSegmentSequence();
            if (Objects.isNull(segmentSequence)) {
                log.warn("Ignoring multipart part without segment sequence for parentId {}", submitSmEvent.getParentId());
                continue;
            }
            if (segmentSequence < startSegment) {
                log.debug("Skipping segment {} of parentId {} because it was already delivered",
                        segmentSequence, submitSmEvent.getParentId());
                continue;
            }

            MessageEvent partEvent = buildEventForMessagePart(submitSmEvent, messagePart);
            if (!sendMessagePart(partEvent)) {
                MultipartPartStatuses.markAsNotSent(submitSmEvent, segmentSequence);
                log.warn("Stopping multipart transmission for parentId {} at segment {} of {}",
                        submitSmEvent.getParentId(), segmentSequence, messagePart.getTotalSegment());
                this.handleMultipartFailure(submitSmEvent, partEvent);
                return;
            }
            MultipartPartStatuses.markAsSent(submitSmEvent, segmentSequence);
        }
        ChargingUtils.publishChargingCompletion(submitSmEvent, true, kafkaTemplate);
    }

    private MessageEvent buildEventForMessagePart(MessageEvent submitSmEvent, MessagePart messagePart) {
        MessageEvent partEvent = Converter.deepCopy(submitSmEvent, MessageEvent.class);
        partEvent.setMessageId(messagePart.getMessageId());
        partEvent.setShortMessage(messagePart.getShortMessage());
        partEvent.setMsgReferenceNumber(messagePart.getMsgReferenceNumber());
        partEvent.setTotalSegment(messagePart.getTotalSegment());
        partEvent.setSegmentSequence(messagePart.getSegmentSequence());
        partEvent.setOptionalParameters(messagePart.getOptionalParameters());
        partEvent.setMessageBytes(messagePart.getPartBytes());
        partEvent.setUdhBytes(messagePart.getUdhBytes());
        partEvent.setUdhRaw(messagePart.getUdhRaw());
        partEvent.addCustomParam(messagePart.getCustomParams());
        this.prepareConcatenatedSmsIfNeeded(partEvent);
        return partEvent;
    }

    private boolean sendMessagePart(MessageEvent partEvent) {
        try {
            this.submitShortMessage(partEvent);
            return true;
        } catch (Exception e) {
            partEvent.setErrorCode(handleException(e, partEvent));
            sendProxyResponse(kafkaTemplate, partEvent, UtilsEnum.CdrStatus.FAILED);
            return false;
        }
    }

    private void handleMultipartFailure(MessageEvent submitSmEvent, MessageEvent failedPartEvent) {
        int errorCode = failedPartEvent.getErrorCode();
        MessageEvent failedPartCdr = Converter.deepCopy(failedPartEvent, MessageEvent.class);
        failedPartCdr.setMessageParts(null);
        this.publishFailedCdr(failedPartCdr);

        submitSmEvent.setErrorCode(errorCode);
        ChargingUtils.publishChargingCompletion(submitSmEvent, false, kafkaTemplate);

        if (submitSmEvent.getValidityPeriod() == 0 || submitSmEvent.isLastRetry()) {
            log.debug("Finishing multipart transaction for message {} with error code {}",
                    submitSmEvent.getMessageId(), errorCode);
            this.handlerFailedMessage(submitSmEvent, errorCode);
            return;
        }

        this.sendToRetryProcess(submitSmEvent, errorCode);
    }

    private void sendMessage(MessageEvent submitSmEvent) {
        try {
            this.submitShortMessage(submitSmEvent);
            ChargingUtils.publishChargingCompletion(submitSmEvent, true, kafkaTemplate);
        } catch (Exception e) {
            handleExceptionForErrorOnSendSubmitSm(submitSmEvent, e);
            sendProxyResponse(kafkaTemplate, submitSmEvent, UtilsEnum.CdrStatus.FAILED);
        }
    }

    private void submitShortMessage(MessageEvent submitSmEvent)
            throws PDUException, ResponseTimeoutException, InvalidResponseException, NegativeResponseException, IOException {
        DataCoding dataCoding = EncodingUtils.getDataCoding(submitSmEvent.getDataCoding());
        int encodingType = SmppUtils.determineEncodingType(submitSmEvent.getDataCoding(), this.gateway);
        byte[] encodedShortMessage = buildMessageBytesToDeliver(submitSmEvent, encodingType);

        OptionalParameter[] parameters = Objects.nonNull(submitSmEvent.getOptionalParameters()) ?
                SmppUtils.getTLV(submitSmEvent) : new OptionalParameter[0];
        SubmitSmResult submitSmResult = this.getRandomSession().submitShortMessage(
                submitSmEvent.getServiceType(),
                UtilsEnum.getTypeOfNumber(submitSmEvent.getSourceAddrTon()),
                UtilsEnum.getNumberingPlanIndicator(submitSmEvent.getSourceAddrNpi()),
                submitSmEvent.getSourceAddr(),
                UtilsEnum.getTypeOfNumber(submitSmEvent.getDestAddrTon()),
                UtilsEnum.getNumberingPlanIndicator(submitSmEvent.getDestAddrNpi()),
                submitSmEvent.getDestinationAddr(),
                new ESMClass(submitSmEvent.getEsmClass()),
                submitSmEvent.getProtocolId(),
                submitSmEvent.getPriorityFlag(),
                submitSmEvent.getScheduleDeliveryTime(),
                submitSmEvent.getStringValidityPeriod(),
                new RegisteredDelivery(submitSmEvent.getRegisteredDelivery()),
                submitSmEvent.getReplaceIfPresent(),
                dataCoding,
                (byte) submitSmEvent.getSmDefaultMsgId(),
                encodedShortMessage,
                parameters);
        sendProxyResponse(kafkaTemplate, submitSmEvent, UtilsEnum.CdrStatus.SUCCESS);
        requestCounterTotal.incrementAndGet();
        addInCache(submitSmEvent.getRegisteredDelivery(), submitSmEvent, submitSmResult);
        cdrProcessor.publishCdr(submitSmEvent, submitSmResult.getMessageId(), UtilsEnum.Module.SMPP_CLIENT,
                UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.SUCCESS, kafkaTemplate);
    }

    private void sendProxyResponse(KafkaTemplate<String, String> kafkaTemplate, MessageEvent event, UtilsEnum.CdrStatus cdrStatus) {
        if (event.isUseProxy()) {
            boolean completedWithError = false;
            String errorMsg = "";
            if (cdrStatus.equals(UtilsEnum.CdrStatus.FAILED)) {
                completedWithError = true;
                errorMsg = ErrorCodes.getErrorDescription(UtilsEnum.Module.SMPP_SERVER, event.getErrorCode(), null);
            }
            UtilsRecords.HttpProxyResponse httpProxyResponse = new UtilsRecords.HttpProxyResponse(event.getMessageId(), completedWithError, event.getErrorCode(), errorMsg, null);
            kafkaTemplate.send(KafkaTopicsConstants.HTTP_PROXY_TOPIC, httpProxyResponse.toString());
        }
    }

    private byte[] buildMessageBytesToDeliver(MessageEvent submitSmEvent, int encodingType) {
        byte[] shortMessage = new byte[0];
        boolean shortMessageContainsData = !isNullOrEmpty(submitSmEvent.getShortMessage());
        boolean messagePayloadTlvIsAbsent = !Utils.containsMessagePayloadTlv(submitSmEvent);
        if (messagePayloadTlvIsAbsent && shortMessageContainsData) {
            shortMessage = EncodingUtils.encodeMessage(submitSmEvent.getShortMessage(), encodingType);
            if (submitSmEvent.getUdhLength() > 0) {
                shortMessage = EncodingUtils.prepend(submitSmEvent.getUdhBytes(), shortMessage);
            }
        }

        return shortMessage;
    }

    private boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }

    private void handleExceptionForErrorOnSendSubmitSm(MessageEvent submitSmEvent, Exception exception) {
        int errorHandlingResult = handleException(exception, submitSmEvent);
        submitSmEvent.setErrorCode(errorHandlingResult);
        log.debug("Error handling result for submit_sm with id {} is {}. " +
                        "Validity period is: {}, isLastRetry: {}",
                submitSmEvent.getMessageId(), errorHandlingResult,
                submitSmEvent.getValidityPeriod(), submitSmEvent.isLastRetry());

        ChargingUtils.publishChargingCompletion(submitSmEvent, false, kafkaTemplate);

        if (submitSmEvent.getValidityPeriod() == 0) {
            this.handlerFailedMessage(submitSmEvent, errorHandlingResult);
            log.debug("An error occurred while sending submit_sm with id {}. The validity period is 0, finishing the transaction", submitSmEvent.getMessageId());
            return;
        }

        if (submitSmEvent.isLastRetry()) {
            this.handlerFailedMessage(submitSmEvent, errorHandlingResult);
            log.debug("An error occurred while sending submit_sm with id {}. The message has reached the validity period, finishing the transaction", submitSmEvent.getMessageId());
            return;
        }

        this.sendToRetryProcess(submitSmEvent, errorHandlingResult);
    }

    private void handlerFailedMessage(MessageEvent submitSmEvent, int errorCode) {
        submitSmEvent.setErrorCode(errorCode);
        this.publishFailedCdr(submitSmEvent);
        this.publishSkippedPartsCdr(submitSmEvent);
        this.sendDeliverSmForFailedCases(submitSmEvent, errorCode);
        this.applyRefundIfNeeded(submitSmEvent);
    }

    private void applyRefundIfNeeded(MessageEvent submitSmEvent) {
        if (ChargingUtils.checkMessageForRefund(submitSmEvent)) {
            log.debug("Applying refund for message with id {}", submitSmEvent.getMessageId());
            String topicByPriority = KafkaUtils.getChargingTopicPriority(submitSmEvent.getSmscMessagePriority());
            this.kafkaTemplate.send(topicByPriority, submitSmEvent.toString());
        }
    }

    private SMPPSession getRandomSession() {
        if (sessions.isEmpty()) {
            throw new NoAvailableSessionException("No active SMPP sessions for networkId = " + gateway.getNetworkId());
        }
        int randomIndex = secureRandom.nextInt(sessions.size());
        SMPPSession selectedSession = sessions.get(randomIndex);
        if (Objects.isNull(selectedSession) || !selectedSession.getSessionState().isBound()) {
            throw new NoAvailableSessionException("Selected session is not bound for networkId = " + gateway.getNetworkId());
        }
        return selectedSession;
    }

    public static String cleanAndUpperString(String messageId) {
        return messageId.replaceAll("[\\s\\p{Cntrl}]+", "").replaceFirst("^0+", "").toUpperCase();
    }

    private void prepareConcatenatedSmsIfNeeded(MessageEvent messageEvent) {
        String splitBy = this.gateway.getSplitSmppType();
        boolean smsContainsInfoAboutConcatenation =
                EncodingUtils.udhContainsConcatenationIei(messageEvent) || Utils.optParamsContainsConcatenationTlv(messageEvent);

        if (!smsContainsInfoAboutConcatenation) {
            if (Objects.equals("UDH", splitBy)) {
                boolean use16Bit = !MSG_REFERENCE_TYPE_8BIT.equalsIgnoreCase(
                        (String) messageEvent.getFromCustomParam(MSG_REFERENCE_TYPE, MSG_REFERENCE_TYPE_8BIT));
                EncodingUtils.applyConcatenationUdhPrefix(messageEvent, use16Bit);
                return;
            }

            Utils.addConcatenatedTlvSmsDetails(messageEvent);
        }
    }
}
