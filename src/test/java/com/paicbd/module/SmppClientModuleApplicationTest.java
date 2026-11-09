package com.paicbd.module;

import com.paicbd.module.e2e.SmppServerMock;
import com.paicbd.module.smpp.SmppConnectionManager;
import com.paicbd.module.utils.AppProperties;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.MessagePart;
import com.paicbd.smsc.dto.Udh;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.scylla.ScyllaTablesConstants;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.UtilsEnum;
import com.paicbd.smsc.ws.SocketSession;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.jsmpp.extra.NegativeResponseException;
import org.jsmpp.extra.ResponseTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.stomp.StompSession;
import com.paicbd.smsc.utils.RedisManager;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.awaitility.Awaitility;

import static com.paicbd.smsc.utils.GeneralSmscConstants.MSG_REFERENCE_TYPE;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SmppClientModuleApplicationTest {
    private static final int PORT = 7777;
    private static final SmppServerMock smppServerMock = new SmppServerMock(PORT);
    private static final ExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @Mock
    private RedisManager redisManager;
    @Mock
    AppProperties appProperties;
    @Mock
    private SocketSession socketSession;
    @Mock
    private ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap;
    @Mock
    private ScyllaManager scyllaManager;
    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;
    @Mock
    private KafkaConsumerFactory kafkaConsumerFactory;

    // Class under test
    private SmppConnectionManager smppConnectionManager;

    @BeforeAll
    static void setUpAll() {
        executorService.submit(smppServerMock);
    }

    @AfterAll
    static void tearDown() {
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("Connect when SMPP server throws an exception")
    void connectWhenSMPPServerThrowsExceptionThenDoNothing() {
        setupAppPropertiesMocks();
        smppServerMock.setForcedSmppServerException(new Exception());
        smppServerMock.setEmptyShortMessage(false);

        Gateway smppGateway = getSMPPGw();
        smppConnectionManager = new SmppConnectionManager(
                redisManager, smppGateway, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        smppConnectionManager.connect();
        assertFalse(smppConnectionManager.getSessions().isEmpty());
    }

    private void setupAppPropertiesMocks() {
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
    }

    @ParameterizedTest
    @MethodSource("exceptionSendMessageParameters")
    @DisplayName("Try to send message when server listener throws an exception")
    void startMessagesProcessorWhenServerListenerThrowsExceptionThenSendToRetries(Exception exception, int errorCode) {
        smppServerMock.setForcedSmppServerException(exception);
        smppServerMock.setForcedListenerException(exception);
        smppServerMock.setEmptyShortMessage(false);

        Gateway smppGateway = getSMPPGw();
        smppGateway.setSessionsNumber(1);
        smppGateway.setAutoRetryErrorCode(errorCode + "");
        MessageEvent messageEvent = getMessageEvent();
        messageEvent.setRetry(true);
        messageEvent.setRetryNumber(1);
        when(socketSession.getStompSession()).thenReturn(mock(StompSession.class));
        setupAppPropertiesMocks();

        smppConnectionManager = spy(new SmppConnectionManager(
                redisManager, smppGateway, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory));

        doReturn(List.of(messageEvent)).doReturn(List.of()).when(smppConnectionManager).fetchMessages(anyInt(), anyInt(), anyInt(), anyInt(), any(), any(), any());

        smppConnectionManager.connect();
        ArgumentCaptor<String> cdrCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageEventCaptor = ArgumentCaptor.forClass(String.class);
        await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    verify(kafkaTemplate, atLeastOnce())
                            .send(eq(KafkaTopicsConstants.CDR_TOPIC), cdrCaptor.capture());

                    verify(kafkaTemplate, atLeastOnce())
                            .send(eq(KafkaTopicsConstants.RETRIES_MEDIUM_TOPIC), messageEventCaptor.capture());
                });

        String cdrInRaw = cdrCaptor.getValue();
        UtilsRecords.Cdr cdrEvent = Converter.stringToObject(cdrInRaw, UtilsRecords.Cdr.class);
        assertEquals(messageEvent.getMessageId(), cdrEvent.messageId());
        assertEquals(UtilsEnum.CdrStatus.FAILED.name(), cdrEvent.status());
        String messageInRaw = messageEventCaptor.getValue();
        MessageEvent messageEventToRetry = Converter.stringToObject(messageInRaw, MessageEvent.class);
        assertEquals(messageEvent.getMessageId(), messageEventToRetry.getMessageId());
        assertTrue(messageEventToRetry.isRetry());
        assertEquals(2, messageEventToRetry.getRetryNumber());
        smppConnectionManager.stopConnection();
    }

    public static ConsumerRecords<String, String> fromSingleRecord(String event) {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("3.smpp.sms", 1, 1L, "key1", event);
        TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
        List<ConsumerRecord<String, String>> recordList = Collections.singletonList(consumerRecord);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        recordsMap.put(topicPartition, recordList);
        return new ConsumerRecords<>(recordsMap);
    }

    @Test
    @DisplayName("Send message when registered delivery is not zero")
    void startMessagesProcessorWhenRegisteredDeliveryIsNotZeroThenCheckSubmitSmValues() {
        smppServerMock.setForcedListenerException(null);
        smppServerMock.setForcedSmppServerException(null);
        smppServerMock.setEmptyShortMessage(false);

        Gateway smppGateway = getSMPPGw();
        MessageEvent messageEvent = getMessageEvent();
        when(socketSession.getStompSession()).thenReturn(mock(StompSession.class));
        setupAppPropertiesMocks();

        smppConnectionManager = spy(new SmppConnectionManager(
                redisManager, smppGateway, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory));

        doReturn(List.of(messageEvent)).doReturn(List.of()).when(smppConnectionManager).fetchMessages(anyInt(), anyInt(), anyInt(), anyInt(), any(), any(), any());

        smppConnectionManager.connect();
        toSleep();
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> fieldCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(scyllaManager, atLeastOnce()).insertIntoTable(keyCaptor.capture(), fieldCaptor.capture(), valueCaptor.capture());
        UtilsRecords.SubmitSmResponseEvent submitSmResponseEvent = Converter.stringToObject(valueCaptor.getValue(), UtilsRecords.SubmitSmResponseEvent.class);
        assertEquals(ScyllaTablesConstants.SMPP_SUBMIT_SM_RESULT_TABLE, keyCaptor.getValue());
        assertEquals(fieldCaptor.getValue(), submitSmResponseEvent.hashId());
        assertEquals(submitSmResponseEvent.submitSmServerId(), messageEvent.getMessageId());
        assertEquals(smppGateway.getSessionsNumber(), smppConnectionManager.getSessions().size());
        smppConnectionManager.stopConnection();
    }

    @Test
    @DisplayName("Send message when message parts is not null")
    void startMessagesProcessorWhenMessagePartsIsNotNullThenCheckSubmitSmValues() {
        setupAppPropertiesMocks();
        smppServerMock.setForcedListenerException(null);
        smppServerMock.setForcedSmppServerException(null);
        smppServerMock.setEmptyShortMessage(false);

        Gateway smppGateway = getSMPPGw();
        smppGateway.setEncodingIso88591(3);
        smppGateway.setEncodingGsm7(1);
        smppGateway.setEncodingUcs2(2);
        String messageId = "1722446896082-12194920127675";
        MessageEvent messageEvent = getMessageEvent();
        messageEvent.setShortMessage("Testing message part I'm the first part");
        messageEvent.setEsmClass(64);
        messageEvent.setUdhRaw(new HashSet<>());
        messageEvent.setMessageParts(
                List.of(
                        MessagePart.builder()
                                .messageId(messageId)
                                .shortMessage("Testing message part I'm the first part")
                                .segmentSequence(1)
                                .totalSegment(2)
                                .msgReferenceNumber("2")
                                .udhRaw(Set.of())
                                .build(),
                        MessagePart.builder()
                                .messageId(messageId)
                                .shortMessage("Testing message part I'm the second part")
                                .segmentSequence(2)
                                .totalSegment(2)
                                .msgReferenceNumber("2")
                                .udhRaw(Set.of())
                                .build()
                )
        );

        smppConnectionManager = spy(new SmppConnectionManager(
                redisManager, smppGateway, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory));

        doReturn(List.of(messageEvent)).doReturn(List.of()).when(smppConnectionManager).fetchMessages(anyInt(), anyInt(), anyInt(), anyInt(), any(), any(), any());

        smppConnectionManager.connect();
        toSleep();
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> fieldCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(scyllaManager, atLeastOnce()).insertIntoTable(keyCaptor.capture(), fieldCaptor.capture(), valueCaptor.capture());
        UtilsRecords.SubmitSmResponseEvent submitSmResponseEvent = Converter.stringToObject(valueCaptor.getValue(), UtilsRecords.SubmitSmResponseEvent.class);
        assertEquals(ScyllaTablesConstants.SMPP_SUBMIT_SM_RESULT_TABLE, keyCaptor.getValue());
        assertEquals(fieldCaptor.getValue(), submitSmResponseEvent.hashId());
        assertEquals(submitSmResponseEvent.submitSmServerId(), messageEvent.getMessageId());
    }

    @ParameterizedTest
    @MethodSource("errorParameters")
    @DisplayName("Try to send message when the error exists in no retry error list")
    void startMessagesProcessorWhenErrorExistsInNoErrorCodeListThenCheckSubmitSmValues(int errorCodeMapping, String originProtocol, Long validityPeriod, boolean isLastRetry) {
        smppServerMock.setForcedListenerException(new NegativeResponseException(8));
        smppServerMock.setForcedSmppServerException(null);
        smppServerMock.setEmptyShortMessage(false);

        Gateway smppGateway = getSMPPGw();
        smppGateway.setSessionsNumber(1);
        MessageEvent messageEvent = getMessageEvent();
        messageEvent.setValidityPeriod(validityPeriod);
        messageEvent.setOriginProtocol(originProtocol);
        messageEvent.setErrorCode(5555);
        messageEvent.setLastRetry(isLastRetry);
        List<ErrorCodeMapping> errorCodeMappings = getErrorCodeMappingList(errorCodeMapping);

        when(errorCodeMappingConcurrentHashMap.get(String.valueOf(smppGateway.getMno()))).thenReturn(errorCodeMappings);
        when(socketSession.getStompSession()).thenReturn(mock(StompSession.class));
        setupAppPropertiesMocks();
        smppConnectionManager = spy(new SmppConnectionManager(
                redisManager, smppGateway, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory));

        doReturn(List.of(messageEvent)).doReturn(List.of()).when(smppConnectionManager).fetchMessages(anyInt(), anyInt(), anyInt(), anyInt(), any(), any(), any());

        smppConnectionManager.connect();
        toSleep();
        if (!originProtocol.equals("ss7")) {
            ArgumentCaptor<String> fieldCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);

            verify(kafkaTemplate, atLeast(2)).send(fieldCaptor.capture(), valueCaptor.capture());
            List<String> capturedValues = valueCaptor.getAllValues();
            MessageEvent event = capturedValues.stream()
                    .map(value -> Converter.stringToObject(value, MessageEvent.class))
                    .filter(msg -> msg != null && msg.getMessageId() != null)
                    .findFirst()
                    .orElse(null);
            assertNotNull(event);
            assertNotNull(event.getMessageId());
        } else {
            verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString());
        }
        smppConnectionManager.stopConnection();
    }

    @ParameterizedTest
    @MethodSource("destinationParameters")
    @DisplayName("Try to send message when error exists in alternate destination list")
    void startMessagesProcessorWhenErrorExistsInAlternateDestinationListThenCheckSubmitSmValues(String destinationProtocol) {
        smppServerMock.setForcedListenerException(new NegativeResponseException(8));
        smppServerMock.setForcedSmppServerException(null);
        smppServerMock.setEmptyShortMessage(false);

        Gateway smppGateway = getSMPPGw();
        smppGateway.setSessionsNumber(1);
        smppGateway.setRetryAlternateDestinationErrorCode("8");
        MessageEvent messageEvent = getMessageEvent();
        messageEvent.setDestProtocol(destinationProtocol);
        messageEvent.setDestNetworkType("GW");
        messageEvent.setDestNetworkId(2);
        when(socketSession.getStompSession()).thenReturn(mock(StompSession.class));
        setupAppPropertiesMocks();
        smppConnectionManager = spy(new SmppConnectionManager(
                redisManager, smppGateway, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory));

        doReturn(List.of(messageEvent)).doReturn(List.of()).when(smppConnectionManager).fetchMessages(anyInt(), anyInt(), anyInt(), anyInt(), any(), any(), any());

        smppConnectionManager.connect();

        ArgumentCaptor<String> cdrCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageEventCaptor = ArgumentCaptor.forClass(String.class);
        await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    verify(kafkaTemplate, atLeastOnce())
                            .send(eq(KafkaTopicsConstants.CDR_TOPIC), cdrCaptor.capture());

                    verify(kafkaTemplate, atLeastOnce())
                            .send(eq(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC), messageEventCaptor.capture());
                });

        String cdrInRaw = cdrCaptor.getValue();
        UtilsRecords.Cdr cdrEvent = Converter.stringToObject(cdrInRaw, UtilsRecords.Cdr.class);
        assertEquals(messageEvent.getMessageId(), cdrEvent.messageId());
        assertEquals(UtilsEnum.CdrStatus.FAILED.name(), cdrEvent.status());
        String messageInRaw = messageEventCaptor.getValue();
        MessageEvent messageEventToRetry = Converter.stringToObject(messageInRaw, MessageEvent.class);
        assertEquals(messageEvent.getMessageId(), messageEventToRetry.getMessageId());
        assertTrue(messageEventToRetry.isRetry());
        assertEquals(1, messageEventToRetry.getRetryNumber());
        smppConnectionManager.stopConnection();
    }

    @ParameterizedTest
    @MethodSource("smsParameters")
    @DisplayName("Send message when registered delivery is not zero")
    void startMessagesAndProcessDlrWithEmptyBody(MessageEvent messageEvent) {
        smppServerMock.setForcedListenerException(null);
        smppServerMock.setForcedSmppServerException(null);
        smppServerMock.setEmptyShortMessage(true);
        Gateway smppGateway = getSMPPGw();
        setupAppPropertiesMocks();
        when(socketSession.getStompSession()).thenReturn(mock(StompSession.class));

        smppConnectionManager = new SmppConnectionManager(
                redisManager, smppGateway, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory);

        assertDoesNotThrow(() -> smppConnectionManager.connect());
        smppConnectionManager.stopConnection();
    }

    private static Stream<Arguments> smsParameters() {
        MessageEvent eventDc0 = getMessageEvent();
        MessageEvent eventDc4 = getMessageEvent();
        MessageEvent eventDc8 = getMessageEvent();
        eventDc4.setDataCoding(4);
        eventDc4.setShortMessage("54455354494E47");
        eventDc8.setDataCoding(8);
        return Stream.of(
                Arguments.of(eventDc0), Arguments.of(eventDc4), Arguments.of(eventDc8));
    }

    private static Stream<Arguments> exceptionSendMessageParameters() {
        return Stream.of(
                Arguments.of(new ResponseTimeoutException(), 300),
                Arguments.of(new NegativeResponseException(8), 8)
        );
    }

    private static Stream<Arguments> errorParameters() {
        return Stream.of(
                //Origin protocol is http, a validity period is 0, and the last retry is false
                Arguments.of(8, "http", 0L, false),
                //Origin protocol is smpp, a validity period is 0, and last retry is false
                Arguments.of(9, "smpp", 0L, false),
                //Origin protocol is ss7, a validity period is 360, and the last retry is true
                Arguments.of(9, "ss7", 360L, true)
        );
    }

    private static Stream<Arguments> destinationParameters() {
        return Stream.of(
                Arguments.of("HTTP"),
                Arguments.of("SMPP"),
                Arguments.of("SS7")
        );
    }

    private static Gateway getSMPPGw() {
        return Gateway.builder()
                .networkId(3)
                .name("smppgw")
                .systemId("smppgw")
                .password("1234")
                .ip("127.0.0.1")
                .port(7777)
                .bindType("TRANSCEIVER")
                .systemType("")
                .interfaceVersion("IF_50")
                .sessionsNumber(1)
                .addressTON(1)
                .addressNPI(4)
                .addressRange("")
                .tps(1)
                .status("STARTED")
                .enabled(1)
                .enquireLinkPeriod(30000)
                .enquireLinkTimeout(5000)
                .requestDLR(1)
                .noRetryErrorCode("65")
                .retryAlternateDestinationErrorCode("640")
                .bindTimeout(5000)
                .bindRetryPeriod(10000)
                .pduTimeout(3000)
                .pduProcessorDegree(1)
                .threadPoolSize(100)
                .mno(1)
                .tlvMessageReceiptId(true)
                .messageIdDecimalFormat(false)
                .protocol("SMPP")
                .autoRetryErrorCode("64,65,66")
                .encodingIso88591(3)
                .encodingGsm7(1)
                .encodingUcs2(2)
                .splitMessage(false)
                .splitSmppType("TLV")
                .messagesPerSecondHigh(70)
                .messagesPerSecondMedium(20)
                .messagesPerSecondLow(10)
                .build();
    }

    private static MessageEvent getMessageEvent() {
        return MessageEvent.builder()
                .id("1722446896082-12194920127675")
                .messageId("1722446896082-12194920127675")
                .parentId("1722446896082-12194920127675")
                .registeredDelivery(1)
                .originNetworkId(6)
                .systemId("smppgw")
                .deliverSmId("1")
                .dataCoding(0)
                .sourceAddrNpi(1)
                .sourceAddrTon(4)
                .sourceAddr("50510201020")
                .originProtocol("SMPP")
                .routingId(3)
                .retryDestNetworkId("")
                .destAddrTon(1)
                .destAddrNpi(1)
                .destinationAddr("50582368999")
                .shortMessage("Testing Message")
                .validityPeriod(160)
                .esmClass(0)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .build();
    }

    private static List<ErrorCodeMapping> getErrorCodeMappingList(Integer errorCode) {
        return List.of(ErrorCodeMapping.builder().errorCode(errorCode).deliveryErrorCode(55).deliveryStatus("DELIVRD").build());
    }

    static void toSleep() {
        try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor()) {
            executorService.schedule(() -> {
            }, 5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest
    @MethodSource("messagePartsBuiltParams")
    void testMessageParts(MessageEvent messageEvent, boolean useUdh) {
        Gateway gateway = getSMPPGw();
        gateway.setSplitSmppType(useUdh ? "UDH" : "SMPP");

        smppServerMock.setForcedSmppServerException(null);
        smppServerMock.setForcedListenerException(null);
        smppServerMock.setEmptyShortMessage(false);

        setupAppPropertiesMocks();
        when(socketSession.getStompSession()).thenReturn(mock(StompSession.class));
        smppConnectionManager = new SmppConnectionManager(
                redisManager, gateway, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory);

        assertDoesNotThrow(() -> smppConnectionManager.connect());
        smppConnectionManager.stopConnection();
    }

    private static Stream<Arguments> messagePartsBuiltParams() {
        MessageEvent smsDc0Udh = buildMessageEvent(
                0,
                "Etiam venenatis sapien semper risus dictum, et ornare dui tincidunt. Aliquam urna orci, venenatis nec cursus quis, maximus sit amet nulla. Curabitur ullamcorper, lacus id auctor eleifend, nisl ante tempor nibh, id gravida mi leo eu turpis. Duis volutpat0.",
                "RXRpYW0gdmVuZW5hdGlzIHNhcGllbiBzZW1wZXIgcmlzdXMgZGljdHVtLCBldCBvcm5hcmUgZHVpIHRpbmNpZHVudC4gQWxpcXVhbSB1cm5hIG9yY2ksIHZlbmVuYXRpcyBuZWMgY3Vyc3VzIHF1aXMsIG1heGltdXMgc2l0IGFtZXQgbnVsbGEuIEN1cmFiaXR1ciB1bGxhbWNvcnBlciwgbGFjdXMgaWQgYXVjdG9yIGVsZWlmZW5kLCBuaXNsIGFudGUgdGVtcG9yIG5pYmgsIGlkIGdyYXZpZGEgbWkgbGVvIGV1IHR1cnBpcy4gRHVpcyB2b2x1dHBhdDAu",
                64,
                "Etiam venenatis sapien semper risus dictum, et ornare dui tincidunt. Aliquam urna orci, venenatis nec cursus quis, maximus sit amet nulla. Curabitur ullamcorper, lacus id auctor eleifend, nisl ante tempor nibh, id gravida mi leo eu turpis. Duis vol",
                "RXRpYW0gdmVuZW5hdGlzIHNhcGllbiBzZW1wZXIgcmlzdXMgZGljdHVtLCBldCBvcm5hcmUgZHVpIHRpbmNpZHVudC4gQWxpcXVhbSB1cm5hIG9yY2ksIHZlbmVuYXRpcyBuZWMgY3Vyc3VzIHF1aXMsIG1heGltdXMgc2l0IGFtZXQgbnVsbGEuIEN1cmFiaXR1ciB1bGxhbWNvcnBlciwgbGFjdXMgaWQgYXVjdG9yIGVsZWlmZW5kLCBuaXNsIGFudGUgdGVtcG9yIG5pYmgsIGlkIGdyYXZpZGEgbWkgbGVvIGV1IHR1cnBpcy4gRHVpcyB2b2w=",
                "utpat0.",
                "dXRwYXQwLg==",
                List.of(),
                new HashSet<>(),
                List.of(),
                new HashSet<>(),
                "",
                "",
                List.of(),
                new HashSet<>()
        );

        MessageEvent smsDc4Udh = buildMessageEvent(
                4,
                "50686173656C6C7573207375736369706974206120616E74652074726973746971756520616C69717565742E204D616563656E6173206672696E67696C6C612070756C76696E6172206C6F72656D20736564206D6178696D75732E20496E206861632068616269746173736520706C617465612064696374756D73742E205175697371756520736564207075727573206D6178696D75732C20616363756D73616E2066656C69732061632C206665726D656E74756D2070757275732E20416C697175616D2073697420616D6574206C696265726F20657420617263752066696E696275732066617563696275732E204E756E6320616320746F72746F72302E",
                "UGhhc2VsbHVzIHN1c2NpcGl0IGEgYW50ZSB0cmlzdGlxdWUgYWxpcXVldC4gTWFlY2VuYXMgZnJpbmdpbGxhIHB1bHZpbmFyIGxvcmVtIHNlZCBtYXhpbXVzLiBJbiBoYWMgaGFiaXRhc3NlIHBsYXRlYSBkaWN0dW1zdC4gUXVpc3F1ZSBzZWQgcHVydXMgbWF4aW11cywgYWNjdW1zYW4gZmVsaXMgYWMsIGZlcm1lbnR1bSBwdXJ1cy4gQWxpcXVhbSBzaXQgYW1ldCBsaWJlcm8gZXQgYXJjdSBmaW5pYnVzIGZhdWNpYnVzLiBOdW5jIGFjIHRvcnRvcjAu",
                64,
                "50686173656C6C7573207375736369706974206120616E74652074726973746971756520616C69717565742E204D616563656E6173206672696E67696C6C612070756C76696E6172206C6F72656D20736564206D6178696D75732E20496E206861632068616269746173736520706C617465612064696374756D73742E205175697371756520736564207075727573206D6178696D75732C20616363756D73616E2066656C69732061632C206665726D656E74756D2070757275732E20416C697175616D2073697420616D6574206C696265726F20657420617263752066696E696275732066617563696275732E204E756E632061632074",
                "UGhhc2VsbHVzIHN1c2NpcGl0IGEgYW50ZSB0cmlzdGlxdWUgYWxpcXVldC4gTWFlY2VuYXMgZnJpbmdpbGxhIHB1bHZpbmFyIGxvcmVtIHNlZCBtYXhpbXVzLiBJbiBoYWMgaGFiaXRhc3NlIHBsYXRlYSBkaWN0dW1zdC4gUXVpc3F1ZSBzZWQgcHVydXMgbWF4aW11cywgYWNjdW1zYW4gZmVsaXMgYWMsIGZlcm1lbnR1bSBwdXJ1cy4gQWxpcXVhbSBzaXQgYW1ldCBsaWJlcm8gZXQgYXJjdSBmaW5pYnVzIGZhdWNpYnVzLiBOdW5jIGFjIHQ=",
                "6F72746F72302E",
                "b3J0b3IwLg==",
                List.of(),
                new HashSet<>(),
                List.of(),
                new HashSet<>(),
                "",
                "",
                List.of(),
                new HashSet<>()
        );

        MessageEvent smsDc8Udh = buildMessageEvent(
                8,
                "Fusce efficitur tincidunt tortor a auctor. Cras a felis vitae elit imperdiet consectetur. Morbi venenatis nulla eget efficitur0.",
                "AEYAdQBzAGMAZQAgAGUAZgBmAGkAYwBpAHQAdQByACAAdABpAG4AYwBpAGQAdQBuAHQAIAB0AG8AcgB0AG8AcgAgAGEAIABhAHUAYwB0AG8AcgAuACAAQwByAGEAcwAgAGEAIABmAGUAbABpAHMAIAB2AGkAdABhAGUAIABlAGwAaQB0ACAAaQBtAHAAZQByAGQAaQBlAHQAIABjAG8AbgBzAGUAYwB0AGUAdAB1AHIALgAgAE0AbwByAGIAaQAgAHYAZQBuAGUAbgBhAHQAaQBzACAAbgB1AGwAbABhACAAZQBnAGUAdAAgAGUAZgBmAGkAYwBpAHQAdQByADAALg==",
                64,
                "Fusce efficitur tincidunt tortor a auctor. Cras a felis vitae elit imperdiet consectetur. Morbi venenatis nulla eget efficit",
                "AEYAdQBzAGMAZQAgAGUAZgBmAGkAYwBpAHQAdQByACAAdABpAG4AYwBpAGQAdQBuAHQAIAB0AG8AcgB0AG8AcgAgAGEAIABhAHUAYwB0AG8AcgAuACAAQwByAGEAcwAgAGEAIABmAGUAbABpAHMAIAB2AGkAdABhAGUAIABlAGwAaQB0ACAAaQBtAHAAZQByAGQAaQBlAHQAIABjAG8AbgBzAGUAYwB0AGUAdAB1AHIALgAgAE0AbwByAGIAaQAgAHYAZQBuAGUAbgBhAHQAaQBzACAAbgB1AGwAbABhACAAZQBnAGUAdAAgAGUAZgBmAGkAYwBpAHQ=",
                "ur0.",
                "AHUAcgAwAC4=",
                List.of(),
                new HashSet<>(),
                List.of(),
                new HashSet<>(),
                "",
                "",
                List.of(),
                new HashSet<>()
        );

        MessageEvent smsDc0Tlv = buildMessageEvent(
                0,
                "Etiam venenatis sapien semper risus dictum, et ornare dui tincidunt. Aliquam urna orci, venenatis nec cursus quis, maximus sit amet nulla. Curabitur ullamcorper, lacus id auctor eleifend, nisl ante tempor nibh, id gravida mi leo eu turpis. Duis volutpat0.",
                "RXRpYW0gdmVuZW5hdGlzIHNhcGllbiBzZW1wZXIgcmlzdXMgZGljdHVtLCBldCBvcm5hcmUgZHVpIHRpbmNpZHVudC4gQWxpcXVhbSB1cm5hIG9yY2ksIHZlbmVuYXRpcyBuZWMgY3Vyc3VzIHF1aXMsIG1heGltdXMgc2l0IGFtZXQgbnVsbGEuIEN1cmFiaXR1ciB1bGxhbWNvcnBlciwgbGFjdXMgaWQgYXVjdG9yIGVsZWlmZW5kLCBuaXNsIGFudGUgdGVtcG9yIG5pYmgsIGlkIGdyYXZpZGEgbWkgbGVvIGV1IHR1cnBpcy4gRHVpcyB2b2x1dHBhdDAu",
                0,
                "Etiam venenatis sapien semper risus dictum, et ornare dui tincidunt. Aliquam urna orci, venenatis nec cursus quis, maximus sit amet nulla. Curabitur ullamcorper, lacus id auctor eleifend, nisl ante tempor nibh, id gravida mi leo eu turpis. Duis volutpat0",
                "RXRpYW0gdmVuZW5hdGlzIHNhcGllbiBzZW1wZXIgcmlzdXMgZGljdHVtLCBldCBvcm5hcmUgZHVpIHRpbmNpZHVudC4gQWxpcXVhbSB1cm5hIG9yY2ksIHZlbmVuYXRpcyBuZWMgY3Vyc3VzIHF1aXMsIG1heGltdXMgc2l0IGFtZXQgbnVsbGEuIEN1cmFiaXR1ciB1bGxhbWNvcnBlciwgbGFjdXMgaWQgYXVjdG9yIGVsZWlmZW5kLCBuaXNsIGFudGUgdGVtcG9yIG5pYmgsIGlkIGdyYXZpZGEgbWkgbGVvIGV1IHR1cnBpcy4gRHVpcyB2b2x1dHBhdDA=",
                ".",
                "Lg==",
                List.of(),
                new HashSet<>(),
                List.of(),
                new HashSet<>(),
                "",
                "",
                List.of(),
                new HashSet<>()
        );

        MessageEvent smsDc4Tlv = buildMessageEvent(
                4,
                "50686173656C6C7573207375736369706974206120616E74652074726973746971756520616C69717565742E204D616563656E6173206672696E67696C6C612070756C76696E6172206C6F72656D20736564206D6178696D75732E20496E206861632068616269746173736520706C617465612064696374756D73742E205175697371756520736564207075727573206D6178696D75732C20616363756D73616E2066656C69732061632C206665726D656E74756D2070757275732E20416C697175616D2073697420616D6574206C696265726F20657420617263752066696E696275732066617563696275732E204E756E6320616320746F72746F72302E",
                "UGhhc2VsbHVzIHN1c2NpcGl0IGEgYW50ZSB0cmlzdGlxdWUgYWxpcXVldC4gTWFlY2VuYXMgZnJpbmdpbGxhIHB1bHZpbmFyIGxvcmVtIHNlZCBtYXhpbXVzLiBJbiBoYWMgaGFiaXRhc3NlIHBsYXRlYSBkaWN0dW1zdC4gUXVpc3F1ZSBzZWQgcHVydXMgbWF4aW11cywgYWNjdW1zYW4gZmVsaXMgYWMsIGZlcm1lbnR1bSBwdXJ1cy4gQWxpcXVhbSBzaXQgYW1ldCBsaWJlcm8gZXQgYXJjdSBmaW5pYnVzIGZhdWNpYnVzLiBOdW5jIGFjIHRvcnRvcjAu",
                0,
                "50686173656C6C7573207375736369706974206120616E74652074726973746971756520616C69717565742E204D616563656E6173206672696E67696C6C612070756C76696E6172206C6F72656D20736564206D6178696D75732E20496E206861632068616269746173736520706C617465612064696374756D73742E205175697371756520736564207075727573206D6178696D75732C20616363756D73616E2066656C69732061632C206665726D656E74756D2070757275732E20416C697175616D2073697420616D6574206C696265726F20657420617263752066696E696275732066617563696275732E204E756E6320616320746F72746F7230",
                "UGhhc2VsbHVzIHN1c2NpcGl0IGEgYW50ZSB0cmlzdGlxdWUgYWxpcXVldC4gTWFlY2VuYXMgZnJpbmdpbGxhIHB1bHZpbmFyIGxvcmVtIHNlZCBtYXhpbXVzLiBJbiBoYWMgaGFiaXRhc3NlIHBsYXRlYSBkaWN0dW1zdC4gUXVpc3F1ZSBzZWQgcHVydXMgbWF4aW11cywgYWNjdW1zYW4gZmVsaXMgYWMsIGZlcm1lbnR1bSBwdXJ1cy4gQWxpcXVhbSBzaXQgYW1ldCBsaWJlcm8gZXQgYXJjdSBmaW5pYnVzIGZhdWNpYnVzLiBOdW5jIGFjIHRvcnRvcjA=",
                "2E",
                "Lg==",
                List.of(),
                new HashSet<>(),
                List.of(),
                new HashSet<>(),
                "",
                "",
                List.of(),
                new HashSet<>()
        );

        MessageEvent smsDc8Tlv = buildMessageEvent(
                8,
                "Fusce efficitur tincidunt tortor a auctor. Cras a felis vitae elit imperdiet consectetur. Morbi venenatis nulla eget efficitur0.",
                "AEYAdQBzAGMAZQAgAGUAZgBmAGkAYwBpAHQAdQByACAAdABpAG4AYwBpAGQAdQBuAHQAIAB0AG8AcgB0AG8AcgAgAGEAIABhAHUAYwB0AG8AcgAuACAAQwByAGEAcwAgAGEAIABmAGUAbABpAHMAIAB2AGkAdABhAGUAIABlAGwAaQB0ACAAaQBtAHAAZQByAGQAaQBlAHQAIABjAG8AbgBzAGUAYwB0AGUAdAB1AHIALgAgAE0AbwByAGIAaQAgAHYAZQBuAGUAbgBhAHQAaQBzACAAbgB1AGwAbABhACAAZQBnAGUAdAAgAGUAZgBmAGkAYwBpAHQAdQByADAALg==",
                0,
                "Fusce efficitur tincidunt tortor a auctor. Cras a felis vitae elit imperdiet consectetur. Morbi venenatis nulla eget efficitur0",
                "AEYAdQBzAGMAZQAgAGUAZgBmAGkAYwBpAHQAdQByACAAdABpAG4AYwBpAGQAdQBuAHQAIAB0AG8AcgB0AG8AcgAgAGEAIABhAHUAYwB0AG8AcgAuACAAQwByAGEAcwAgAGEAIABmAGUAbABpAHMAIAB2AGkAdABhAGUAIABlAGwAaQB0ACAAaQBtAHAAZQByAGQAaQBlAHQAIABjAG8AbgBzAGUAYwB0AGUAdAB1AHIALgAgAE0AbwByAGIAaQAgAHYAZQBuAGUAbgBhAHQAaQBzACAAbgB1AGwAbABhACAAZQBnAGUAdAAgAGUAZgBmAGkAYwBpAHQAdQByADA=",
                ".",
                "AC4=",
                List.of(),
                new HashSet<>(),
                List.of(),
                new HashSet<>(),
                "",
                "",
                List.of(),
                new HashSet<>()
        );


        var smsDc0MessagePayloadSplitByUdhFirstPartParams = List.of(
                new UtilsRecords.OptionalParameter("1060", "050003050201517569737175652070656C6C656E746573717565206E696268206D61676E612C2073656420706F7274612074656C6C75732066617563696275732061632E20566573746962756C756D2061632070656C6C656E7465737175652065726F732E20446F6E656320616363756D73616E20656666696369747572206A7573746F2061207472697374697175652E20496E206861632068616269746173736520706C617465612064696374756D73742E20437261732073656D706572206C6F72656D206575206C616F72656574207661726975732E2053656420696E2066656C69732073697420616D6574206E69736C20766F6C75747061742073656D7065722E20"));
        var smsDc0MessagePayloadSplitByUdhFirstPartUdhRaw = Set.of(new Udh("00", "0003050201"));
        var smsDc0MessagePayloadSplitByUdhSecondPartParams = List.of(
                new UtilsRecords.OptionalParameter("1060", "05000305020250726F696E20696D706572646965742076697665727261206A7573746F206E6F6E206D6178696D75732E20457469616D20636F6E7365637465747572206665756769617420656E696D206120636F6D6D6F646F2E20496E206D617373612072697375732C2068656E6472657269742065676574206C656374757320656765742C20636F6E736571756174206461706962757320697073756D2E20446F6E6563206665726D656E74756D207475727069732076656C206475692064617069627573206665726D656E74756D2E20457469616D2061632072697375732065676574206A7573746F20636F6E64696D656E74756D2065726F732E"));
        var smsDc0MessagePayloadSplitByUdhSecondPartUdhRaw = Set.of(new Udh("00", "0003050202"));
        MessageEvent smsDc0MessagePayloadSplitByUdh = buildMessageEvent(
                0,
                "",
                "",
                64,
                "Quisque pellentesque nibh magna, sed porta tellus faucibus ac. Vestibulum ac pellentesque eros. Donec accumsan efficitur justo a tristique. In hac habitasse platea dictumst. Cras semper lorem eu laoreet varius. Sed in felis sit amet nisl volutpat semper. ",
                "UXVpc3F1ZSBwZWxsZW50ZXNxdWUgbmliaCBtYWduYSwgc2VkIHBvcnRhIHRlbGx1cyBmYXVjaWJ1cyBhYy4gVmVzdGlidWx1bSBhYyBwZWxsZW50ZXNxdWUgZXJvcy4gRG9uZWMgYWNjdW1zYW4gZWZmaWNpdHVyIGp1c3RvIGEgdHJpc3RpcXVlLiBJbiBoYWMgaGFiaXRhc3NlIHBsYXRlYSBkaWN0dW1zdC4gQ3JhcyBzZW1wZXIgbG9yZW0gZXUgbGFvcmVldCB2YXJpdXMuIFNlZCBpbiBmZWxpcyBzaXQgYW1ldCBuaXNsIHZvbHV0cGF0IHNlbXBlci4g",
                "Proin imperdiet viverra justo non maximus. Etiam consectetur feugiat enim a commodo. In massa risus, hendrerit eget lectus eget, consequat dapibus ipsum. Donec fermentum turpis vel dui dapibus fermentum. Etiam ac risus eget justo condimentum eros.",
                "UHJvaW4gaW1wZXJkaWV0IHZpdmVycmEganVzdG8gbm9uIG1heGltdXMuIEV0aWFtIGNvbnNlY3RldHVyIGZldWdpYXQgZW5pbSBhIGNvbW1vZG8uIEluIG1hc3NhIHJpc3VzLCBoZW5kcmVyaXQgZWdldCBsZWN0dXMgZWdldCwgY29uc2VxdWF0IGRhcGlidXMgaXBzdW0uIERvbmVjIGZlcm1lbnR1bSB0dXJwaXMgdmVsIGR1aSBkYXBpYnVzIGZlcm1lbnR1bS4gRXRpYW0gYWMgcmlzdXMgZWdldCBqdXN0byBjb25kaW1lbnR1bSBlcm9zLg==",
                smsDc0MessagePayloadSplitByUdhFirstPartParams,
                smsDc0MessagePayloadSplitByUdhFirstPartUdhRaw,
                smsDc0MessagePayloadSplitByUdhSecondPartParams,
                smsDc0MessagePayloadSplitByUdhSecondPartUdhRaw,
                "BQADBQIB",
                "BQADBQIC",
                List.of(new UtilsRecords.OptionalParameter("1060", "050003050201517569737175652070656C6C656E746573717565206E696268206D61676E612C2073656420706F7274612074656C6C75732066617563696275732061632E20566573746962756C756D2061632070656C6C656E7465737175652065726F732E20446F6E656320616363756D73616E20656666696369747572206A7573746F2061207472697374697175652E20496E206861632068616269746173736520706C617465612064696374756D73742E20437261732073656D706572206C6F72656D206575206C616F72656574207661726975732E2053656420696E2066656C69732073697420616D6574206E69736C20766F6C75747061742073656D7065722E20")),
                Set.of()

        );

        var smsDc0MessagePayloadSplitByTlvFirstPartParams = List.of(
                new UtilsRecords.OptionalParameter("1060", "517569737175652070656C6C656E746573717565206E696268206D61676E612C2073656420706F7274612074656C6C75732066617563696275732061632E20566573746962756C756D2061632070656C6C656E7465737175652065726F732E20446F6E656320616363756D73616E20656666696369747572206A7573746F2061207472697374697175652E20496E206861632068616269746173736520706C617465612064696374756D73742E20437261732073656D706572206C6F72656D206575206C616F72656574207661726975732E2053656420696E2066656C69732073697420616D6574206E69736C20766F6C75747061742073656D7065722E20"),
                new UtilsRecords.OptionalParameter("524", "1"),
                new UtilsRecords.OptionalParameter("526", "2"),
                new UtilsRecords.OptionalParameter("527", "1"));
        var smsDc0MessagePayloadSplitByTlvSecondPartParams = List.of(
                new UtilsRecords.OptionalParameter("524", "1"),
                new UtilsRecords.OptionalParameter("526", "2"),
                new UtilsRecords.OptionalParameter("527", "2"));

        MessageEvent smsDc0MessagePayloadSplitByTlv = buildMessageEvent(
                0,
                "",
                "",
                3,
                "Quisque pellentesque nibh magna, sed porta tellus faucibus ac. Vestibulum ac pellentesque eros. Donec accumsan efficitur justo a tristique. In hac habitasse platea dictumst. Cras semper lorem eu laoreet varius. Sed in felis sit amet nisl volutpat semper. ",
                "UXVpc3F1ZSBwZWxsZW50ZXNxdWUgbmliaCBtYWduYSwgc2VkIHBvcnRhIHRlbGx1cyBmYXVjaWJ1cyBhYy4gVmVzdGlidWx1bSBhYyBwZWxsZW50ZXNxdWUgZXJvcy4gRG9uZWMgYWNjdW1zYW4gZWZmaWNpdHVyIGp1c3RvIGEgdHJpc3RpcXVlLiBJbiBoYWMgaGFiaXRhc3NlIHBsYXRlYSBkaWN0dW1zdC4gQ3JhcyBzZW1wZXIgbG9yZW0gZXUgbGFvcmVldCB2YXJpdXMuIFNlZCBpbiBmZWxpcyBzaXQgYW1ldCBuaXNsIHZvbHV0cGF0IHNlbXBlci4g",
                "Proin imperdiet viverra justo non maximus. Etiam consectetur feugiat enim a commodo. In massa risus, hendrerit eget lectus eget, consequat dapibus ipsum. Donec fermentum turpis vel dui dapibus fermentum. Etiam ac risus eget justo condimentum eros.",
                "UHJvaW4gaW1wZXJkaWV0IHZpdmVycmEganVzdG8gbm9uIG1heGltdXMuIEV0aWFtIGNvbnNlY3RldHVyIGZldWdpYXQgZW5pbSBhIGNvbW1vZG8uIEluIG1hc3NhIHJpc3VzLCBoZW5kcmVyaXQgZWdldCBsZWN0dXMgZWdldCwgY29uc2VxdWF0IGRhcGlidXMgaXBzdW0uIERvbmVjIGZlcm1lbnR1bSB0dXJwaXMgdmVsIGR1aSBkYXBpYnVzIGZlcm1lbnR1bS4gRXRpYW0gYWMgcmlzdXMgZWdldCBqdXN0byBjb25kaW1lbnR1bSBlcm9zLg==",
                smsDc0MessagePayloadSplitByTlvFirstPartParams,
                Set.of(),
                smsDc0MessagePayloadSplitByTlvSecondPartParams,
                Set.of(),
                "",
                "",
                List.of(),
                Set.of()
        );

        return Stream.of(
                Arguments.of(smsDc0Udh, true),
                Arguments.of(smsDc4Udh, true),
                Arguments.of(smsDc8Udh, true),
                Arguments.of(smsDc0Tlv, false),
                Arguments.of(smsDc4Tlv, false),
                Arguments.of(smsDc8Tlv, false),
                Arguments.of(smsDc0MessagePayloadSplitByUdh, true),
                Arguments.of(smsDc0MessagePayloadSplitByTlv, false)
        );
    }

    private static MessageEvent buildMessageEvent(
            int dataCoding, String completeSm, String completeSmBytes, int esmClass,
            String firstPartSm, String firstPartBytes,
            String secondPartSm, String secondPartBytes,
            List<UtilsRecords.OptionalParameter> firstPartParameters, Set<Udh> firstPartUdhRaw,
            List<UtilsRecords.OptionalParameter> secondPartParameters, Set<Udh> secondPartUdhRaw,
            String firstPartUdhBytes, String secondPartUdhBytes,
            List<UtilsRecords.OptionalParameter> globalParameters, Set<Udh> globalUdhRaw) {
        MessagePart firstMessagePart = MessagePart.builder()
                .messageId("1750190439835-25489780813591")
                .msgReferenceNumber("1")
                .segmentSequence(1)
                .totalSegment(2)
                .optionalParameters(firstPartParameters)
                .udhRaw(firstPartUdhRaw)
                .shortMessage(firstPartSm)
                .partBytes(Base64.getDecoder().decode(firstPartBytes))
                .udhBytes(Base64.getDecoder().decode(firstPartUdhBytes))
                .build();
        MessagePart secondMessagePart = MessagePart.builder()
                .messageId("1750190439835-25489780813591")
                .msgReferenceNumber("1")
                .segmentSequence(2)
                .totalSegment(2)
                .optionalParameters(secondPartParameters)
                .udhRaw(secondPartUdhRaw)
                .shortMessage(secondPartSm)
                .partBytes(Base64.getDecoder().decode(secondPartBytes))
                .udhBytes(Base64.getDecoder().decode(secondPartUdhBytes))
                .build();

        return MessageEvent.builder()
                .id("1750190439835-25489780813591")
                .messageId("1750190439835-25489780813591")
                .parentId("1750190439835-25489780813591")
                .registeredDelivery(0)
                .originNetworkId(4)
                .systemId("HTTP_SP_01")
                .dataCoding(dataCoding)
                .sourceAddrNpi(0)
                .sourceAddrTon(5)
                .sourceAddr("50510201020")
                .originProtocol("HTTP")
                .originNetworkType("SP")
                .destProtocol("SMPP")
                .destNetworkType("GW")
                .routingId(1)
                .destAddrTon(1)
                .destAddrNpi(1)
                .destinationAddr("50582368999")
                .stringValidityPeriod("000000000100000R")
                .shortMessage(completeSm)
                .messageBytes(Base64.getDecoder().decode(completeSmBytes))
                .validityPeriod(160)
                .esmClass(esmClass)
                .checkSubmitSmResponse(true)
                .udhRaw(new HashSet<>())
                .messageParts(List.of(firstMessagePart, secondMessagePart))
                .optionalParameters(globalParameters)
                .udhRaw(globalUdhRaw)
                .build();
    }

    @ParameterizedTest
    @MethodSource("referenceTypeParams")
    @DisplayName("prepareConcatenatedSmsIfNeeded should apply correct UDH for configured reference type")
    void prepareConcatenatedSmsIfNeededShouldApplyCorrectUdh(String refType, int expectedUdhl, byte expectedIei) throws Exception {
        Gateway gateway = getSMPPGw();
        gateway.setSplitSmppType("UDH");
        smppConnectionManager = new SmppConnectionManager(
                redisManager, gateway, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory);

        MessageEvent event = MessageEvent.builder()
                .messageId("test-" + refType.toLowerCase())
                .msgReferenceNumber("42")
                .totalSegment(3)
                .segmentSequence(1)
                .dataCoding(0)
                .messageBytes(new byte[]{0x48, 0x65, 0x6C, 0x6C, 0x6F})
                .customParams(new HashMap<>(Map.of(MSG_REFERENCE_TYPE, refType)))
                .build();

        Method method = SmppConnectionManager.class.getDeclaredMethod("prepareConcatenatedSmsIfNeeded", MessageEvent.class);
        method.setAccessible(true);
        method.invoke(smppConnectionManager, event);

        assertEquals(expectedUdhl, event.getUdhLength());
        assertEquals(expectedIei, event.getUdhBytes()[1]);
        assertEquals(expectedUdhl + 1 + 5, event.getMessageBytes().length);
    }

    static Stream<Arguments> referenceTypeParams() {
        return Stream.of(
                Arguments.of("8BIT", 5, (byte) 0x00),
                Arguments.of("16BIT", 6, (byte) 0x08)
        );
    }

    @Test
    @DisplayName("Connect with TLS enabled and blank truststore path leaves sessions empty")
    void connectWithTlsEnabledAndBlankTruststorePathLeavesSessionsEmpty() {
        Gateway tlsGateway = getSMPPGw();
        tlsGateway.setTlsEnabled(true);

        when(appProperties.getTlsTruststorePath()).thenReturn("");

        smppConnectionManager = new SmppConnectionManager(
                redisManager, tlsGateway, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory);

        smppConnectionManager.connect();

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertTrue(smppConnectionManager.getSessions().isEmpty()));
    }
}
