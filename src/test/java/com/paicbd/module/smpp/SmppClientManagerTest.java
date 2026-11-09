package com.paicbd.module.smpp;

import com.paicbd.module.utils.AppProperties;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.exception.NoAvailableSessionException;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.ChargingType;
import com.paicbd.smsc.utils.ChargingUtils;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.ws.SocketSession;
import org.jsmpp.session.SMPPSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.stomp.StompSession;
import com.paicbd.smsc.utils.RedisManager;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.paicbd.module.utils.Constants.PARAM_UPDATE_STATUS;
import static com.paicbd.module.utils.Constants.STOPPED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SmppClientManagerTest {
    @Mock
    private RedisManager redisManager;
    @Mock
    private AppProperties appProperties;
    @Mock
    private SocketSession socketSession;
    @Mock
    private ConcurrentMap<Integer, SmppConnectionManager> smppConnectionManagerList;
    @Mock
    private ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap;
    @Mock
    private ScyllaManager scyllaManager;
    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;
    @Mock
    private KafkaConsumerFactory kafkaConsumerFactory;

    @InjectMocks
    SmppClientManager smppClientManager;

    @Test
    @DisplayName("Init When Empty Lists Then Success")
    void initWhenEmptyListsThenSuccess() {
        var realSmppConnectionManagerList = new ConcurrentHashMap<Integer, SmppConnectionManager>();
        var realErrorCodeMappingConcurrentHashMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();

        var smppConnectionManagerListFake = spy(realSmppConnectionManagerList);
        var errorCodeMappingConcurrentHashMapFake = spy(realErrorCodeMappingConcurrentHashMap);

        var clientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerListFake,
                errorCodeMappingConcurrentHashMapFake,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        var spy = spy(clientManager);
        spy.startManager();

        assertEquals(0, smppConnectionManagerListFake.size());
        assertEquals(0, errorCodeMappingConcurrentHashMapFake.size());
    }

    @Test
    @DisplayName("Init When Data In Redis Then Load Maps Successfully")
    void initWhenDataInRedisThenLoadMapsSuccessfully() {
        lenient().when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        SmppConnectionManager smppConnectionManager = new SmppConnectionManager(redisManager, Gateway.builder()
                .networkId(1)
                .systemId("op_01_smpp_gw")
                .password("1234")
                .mno(1)
                .tps(1)
                .protocol("SMPP")
                .build(), socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory);

        ErrorCodeMapping errorCodeMapping = ErrorCodeMapping.builder()
                .errorCode(88)
                .deliveryErrorCode(0)
                .deliveryStatus("UNDELIV")
                .build();

        var realSmppConnectionManagerList = new ConcurrentHashMap<Integer, SmppConnectionManager>();
        var realErrorCodeMappingConcurrentHashMap = new ConcurrentHashMap<String, List<ErrorCodeMapping>>();

        realSmppConnectionManagerList.put(1, smppConnectionManager);
        realErrorCodeMappingConcurrentHashMap.put("1", List.of(errorCodeMapping));

        var smppConnectionManagerListFake = spy(realSmppConnectionManagerList);
        var errorCodeMappingConcurrentHashMapFake = spy(realErrorCodeMappingConcurrentHashMap);

        var clientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerListFake,
                errorCodeMappingConcurrentHashMapFake,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );


        lenient().when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        when(redisManager.hgetAll(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME)).thenReturn(
                Map.of("1", smppConnectionManager.getGateway().toString(),
                        "2", Gateway.builder().networkId(2).protocol("HTTP").build().toString()));
        when(redisManager.hgetAll(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME)).thenReturn(Map.of("1", Converter.valueAsString(List.of(errorCodeMapping))));

        var spy = spy(clientManager);
        spy.startManager();

        assertEquals(1, smppConnectionManagerListFake.size());
        assertEquals(1, errorCodeMappingConcurrentHashMapFake.size());
    }

    @Test
    @DisplayName("Update Gateway Non Existing In Redis Then Do Nothing")
    void updateGatewayWhenNonExistingInRedisThenDoNothing() {
        when(redisManager.hget(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME, "1")).thenReturn(null);

        var spy = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerList,
                errorCodeMappingConcurrentHashMap,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );

        spy.updateGateway("1");
        verify(redisManager).hget(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME, "1");

        verifyNoInteractions(socketSession);
        verifyNoInteractions(smppConnectionManagerList);
    }

    @Test
    @DisplayName("Update Gateway Existing In Redis But Not SMPP, Not Put Or Update Gateway")
    void updateGatewayWhenExistingInRedisButNotSmppThenNotPutOrUpdateGateway() {
        Gateway httpGateway = Gateway.builder()
                .protocol("HTTP")
                .networkId(1)
                .build();

        when(redisManager.hget(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME, "1")).thenReturn(httpGateway.toString());

        var spy = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerList,
                errorCodeMappingConcurrentHashMap,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );

        spy.updateGateway("1");
        verify(redisManager).hget("gateways", "1");

        verifyNoInteractions(socketSession);
        verifyNoInteractions(smppConnectionManagerList);
    }

    @Test
    @DisplayName("Update Gateway Existing In Redis And Is SMPP, Put Or Update Gateway")
    void updateGatewayWhenExistingInRedisAndIsSmppButKeyNonExistentInSmppConnectionManagerListThenSuccess() {
        Gateway smppGateway = Gateway.builder()
                .networkId(1)
                .systemId("op_01_smpp_gw")
                .password("1234")
                .mno(1)
                .tps(1)
                .protocol("SMPP")
                .build();

        when(redisManager.hget(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME, "1")).thenReturn(smppGateway.toString());
        lenient().when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");

        ConcurrentMap<Integer, SmppConnectionManager> realMap = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, SmppConnectionManager> smppConnectionManagerListSpy = spy(realMap);

        smppClientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerListSpy,
                errorCodeMappingConcurrentHashMap,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );
        SmppClientManager spy = spy(smppClientManager);

        spy.updateGateway("1");
        verify(redisManager).hget("gateways", "1");

        ArgumentCaptor<Integer> keyCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<SmppConnectionManager> valueCaptor = ArgumentCaptor.forClass(SmppConnectionManager.class);
        verify(smppConnectionManagerListSpy).put(keyCaptor.capture(), valueCaptor.capture());

        assertEquals(valueCaptor.getValue(), smppConnectionManagerListSpy.get(1));
        assertTrue(smppConnectionManagerListSpy.containsKey(keyCaptor.getValue()));

        assertNotNull(smppConnectionManagerListSpy.get(1));
        assertEquals(1, smppConnectionManagerListSpy.size());
        assertEquals(1, smppConnectionManagerListSpy.get(1).getGateway().getNetworkId());
        assertEquals("op_01_smpp_gw", smppConnectionManagerListSpy.get(1).getGateway().getSystemId());
        assertEquals("1234", smppConnectionManagerListSpy.get(1).getGateway().getPassword());
        assertEquals(1, smppConnectionManagerListSpy.get(1).getGateway().getMno());
        assertEquals("SMPP", smppConnectionManagerListSpy.get(1).getGateway().getProtocol());

    }

    @Test
    @DisplayName("Update Gateway Existing In Redis And Is SMPP, Put Or Update Gateway")
    void updateGatewayWhenExistingInRedisAndIsSmppButKeyExistentInSmppConnectionManagerListThenSuccess() {
        Gateway previousSmppGateway = Gateway.builder()
                .networkId(1)
                .name("SMPP-Operator before update")
                .systemId("op_01_smpp_gw")
                .password("1234")
                .mno(1)
                .tps(1)
                .protocol("SMPP")
                .addressRange("")
                .build();
        lenient().when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        when(appProperties.getTimeRetry()).thenReturn(30000L);
        ConcurrentMap<Integer, SmppConnectionManager> realMap = new ConcurrentHashMap<>();
        realMap.put(1, new SmppConnectionManager(
                redisManager, previousSmppGateway, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory));
        ConcurrentMap<Integer, SmppConnectionManager> smppConnectionManagerListSpy = spy(realMap);

        Gateway updatedSmppGateway = Gateway.builder()
                .networkId(1)
                .name("SMPP-Operator after update")
                .systemId("op_01_smpp_gw")
                .password("1234")
                .mno(2)
                .protocol("SMPP")
                .pduProcessorDegree(10)
                .threadPoolSize(100)
                .bindRetryPeriod(30000)
                .addressRange("1234")
                .build();
        when(redisManager.hget(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME, "1")).thenReturn(updatedSmppGateway.toString());

        smppClientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerListSpy,
                errorCodeMappingConcurrentHashMap,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );
        SmppClientManager spy = spy(smppClientManager);

        spy.updateGateway("1");
        verify(redisManager).hget("gateways", "1");

        assertNotNull(smppConnectionManagerListSpy.get(1));
        assertEquals(1, smppConnectionManagerListSpy.size());
        assertEquals(1, smppConnectionManagerListSpy.get(1).getGateway().getNetworkId());

        assertEquals(2, smppConnectionManagerListSpy.get(1).getGateway().getMno());
        assertNotEquals(previousSmppGateway.toString(), smppConnectionManagerListSpy.get(1).getGateway().toString());
        assertNotEquals(previousSmppGateway.getMno(), smppConnectionManagerListSpy.get(1).getGateway().getMno());
        assertNotEquals(previousSmppGateway.getName(), smppConnectionManagerListSpy.get(1).getGateway().getName());
        assertNotEquals(previousSmppGateway.getPduProcessorDegree(), smppConnectionManagerListSpy.get(1).getGateway().getPduProcessorDegree());
        assertNotEquals(previousSmppGateway.getThreadPoolSize(), smppConnectionManagerListSpy.get(1).getGateway().getThreadPoolSize());
        assertNotEquals(previousSmppGateway.getBindRetryPeriod(), smppConnectionManagerListSpy.get(1).getGateway().getBindRetryPeriod());
        assertNotEquals(previousSmppGateway.getAddressRange(), smppConnectionManagerListSpy.get(1).getGateway().getAddressRange());

    }

    @Test
    @DisplayName("Connect Gateway When Gateway Not Exists Then Do Nothing")
    void connectGatewayWhenGatewayNotExistsThenDoNothing() {
        when(smppConnectionManagerList.get(1)).thenReturn(null);
        var clientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerList,
                errorCodeMappingConcurrentHashMap,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );
        var spy = spy(clientManager);

        spy.connectGateway("1");
        verify(smppConnectionManagerList).get(1);

        verifyNoInteractions(socketSession);
    }

    @Test
    @DisplayName("Connect Gateway When Gateway Exists Then Success")
    void connectGatewayWhenGatewayExistsThenSuccess() {
        Gateway gateway = Gateway.builder()
                .networkId(1)
                .systemId("op_01_smpp_gw")
                .password("1234")
                .status(STOPPED)
                .enabled(0)
                .mno(1)
                .tps(1)
                .protocol("SMPP")
                .bindRetryPeriod(30000)
                .build();

        lenient().when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        SmppConnectionManager smppConnectionManager = new SmppConnectionManager(
                redisManager, gateway, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        var spySmppConnectionManager = spy(smppConnectionManager);
        when(smppConnectionManagerList.get(1)).thenReturn(spySmppConnectionManager);

        var clientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerList,
                errorCodeMappingConcurrentHashMap,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );
        var spy = spy(clientManager);

        spy.connectGateway("1");
        verify(spySmppConnectionManager).connect();
        assertEquals("STARTED", smppConnectionManager.getGateway().getStatus());
        assertEquals(1, smppConnectionManager.getGateway().getEnabled());
    }

    @Test
    @DisplayName("Stop Gateway When Gateway Not Exists Then Do Nothing")
    void stopGatewayWhenGatewayNotExistsThenDoNothing() {
        when(smppConnectionManagerList.get(1)).thenReturn(null);
        var clientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerList,
                errorCodeMappingConcurrentHashMap,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );
        var spy = spy(clientManager);

        spy.stopGateway("1");
        verify(smppConnectionManagerList).get(1);

        verifyNoInteractions(socketSession);
    }

    @Test
    @DisplayName("Stop Gateway When Gateway Exists Then Success")
    void stopGatewayWhenGatewayExistsThenSuccess() {
        Gateway gateway = Gateway.builder()
                .networkId(1)
                .systemId("op_01_smpp_gw")
                .password("1234")
                .status("STARTED")
                .enabled(1)
                .mno(1)
                .tps(1)
                .protocol("SMPP")
                .build();

        lenient().when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        SmppConnectionManager smppConnectionManager = new SmppConnectionManager(redisManager, gateway, socketSession,
                errorCodeMappingConcurrentHashMap,                 appProperties , scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        var spySmppConnectionManager = spy(smppConnectionManager);
        when(smppConnectionManagerList.get(1)).thenReturn(spySmppConnectionManager);
        when(socketSession.getStompSession()).thenReturn(mock(StompSession.class));

        var clientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerList,
                errorCodeMappingConcurrentHashMap,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );
        var spy = spy(clientManager);

        spy.stopGateway("1");
        verify(socketSession).sendStatus("1", PARAM_UPDATE_STATUS, STOPPED);
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("Update Error Code Mapping When ErrorCode Mapping Exists Then Update")
    void updateErrorCodeMappingWhenErrorCodeMappingNotExistsThenDoNothing() {
        when(redisManager.hget(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME, "1")).thenReturn(null);

        smppClientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerList,
                errorCodeMappingConcurrentHashMap,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );

        smppClientManager.updateErrorCodeMapping("1");

        verify(redisManager).hget(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME, "1");
        verify(errorCodeMappingConcurrentHashMap).remove("1");
        verify(errorCodeMappingConcurrentHashMap, never()).put("1", eq(any(List.class)));
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("Update Error Code Mapping When ErrorCode Mapping Exists Then Update")
    void updateErrorCodeMappingWhenErrorCodeMappingExistsThenUpdate() {
        ErrorCodeMapping errorCodeMapping = ErrorCodeMapping.builder()
                .errorCode(88)
                .deliveryErrorCode(0)
                .deliveryStatus("UNDELIV")
                .build();
        List<ErrorCodeMapping> errorCodeMappingList = List.of(errorCodeMapping);

        when(redisManager.hget(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME, "1"))
                .thenReturn(Converter.valueAsString(errorCodeMappingList));

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<List<ErrorCodeMapping>> captor = ArgumentCaptor.forClass(List.class);
        smppClientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerList,
                errorCodeMappingConcurrentHashMap,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );

        smppClientManager.updateErrorCodeMapping("1");
        verify(redisManager).hget(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME, "1");
        verify(errorCodeMappingConcurrentHashMap, never()).remove("1");
        verify(errorCodeMappingConcurrentHashMap).put(keyCaptor.capture(), captor.capture());
        assertEquals(Converter.valueAsString(errorCodeMappingList), Converter.valueAsString(captor.getValue()));
    }

    @Test
    @DisplayName("Update Routing Rules When Routing Rules Not Exists Then Do Nothing")
    void onDeleteGatewayWhenGatewayNotExistsThenDoNothing() {
        String stringNetworkId = "2";
        int networkId = Integer.parseInt(stringNetworkId);
        SmppConnectionManager smppConnectionManager = mock(SmppConnectionManager.class);
        smppConnectionManagerList = spy(new ConcurrentHashMap<>());

        smppClientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerList,
                errorCodeMappingConcurrentHashMap,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );

        smppClientManager.deleteGateway(stringNetworkId);
        verify(smppConnectionManagerList, never()).remove(networkId);
        verify(smppConnectionManager, never()).stopConnection();
    }

    @Test
    @DisplayName("On Delete Gateway When Gateway Exists Then Remove From List And Stop Connection")
    void onDeleteGatewayWhenGatewayExistsThenRemoveFromListAndStopConnection() {
        String stringNetworkId = "1";
        int networkId = Integer.parseInt(stringNetworkId);
        var smppConnectionManager = mock(SmppConnectionManager.class);

        smppClientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerList,
                errorCodeMappingConcurrentHashMap,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );

        when(smppConnectionManagerList.get(networkId)).thenReturn(smppConnectionManager);
        smppClientManager.deleteGateway(stringNetworkId);
        verify(smppConnectionManagerList).remove(networkId);
    }


    @Test
    @DisplayName("Pre Destroy When Called Then Stop All Connections")
    void preDestroyWhenCalledThenStopAllConnections() {
        Gateway gw1 = Gateway.builder()
                .networkId(1)
                .systemId("op_01_smpp_gw")
                .password("1234")
                .mno(1)
                .tps(1)
                .protocol("SMPP")
                .build();

        Gateway gw2 = Gateway.builder()
                .networkId(2)
                .systemId("op_02_smpp_gw")
                .password("1234")
                .mno(1)
                .tps(1)
                .protocol("SMPP")
                .build();

        lenient().when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        SmppConnectionManager smppConnectionManager1 = new SmppConnectionManager(
                redisManager, gw1, socketSession, errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        SmppConnectionManager smppConnectionManager2 = new SmppConnectionManager(redisManager, gw2, socketSession,
                errorCodeMappingConcurrentHashMap, appProperties,
                scyllaManager, kafkaTemplate, kafkaConsumerFactory);

        smppConnectionManager1 = spy(smppConnectionManager1);
        smppConnectionManager2 = spy(smppConnectionManager2);

        smppConnectionManagerList = spy(new ConcurrentHashMap<>());
        smppConnectionManagerList.put(1, smppConnectionManager1);
        smppConnectionManagerList.put(2, smppConnectionManager2);

        when(socketSession.getStompSession()).thenReturn(mock(StompSession.class));

        smppClientManager = new SmppClientManager(
                redisManager,
                appProperties,
                socketSession,
                smppConnectionManagerList,
                errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );

        smppClientManager.stopAllConnections();

        verify(smppConnectionManager1).stopConnection();
        verify(smppConnectionManager2).stopConnection();
    }


    static Stream<String> priorityProvider() {
        return Stream.of(
                GeneralSmscConstants.HIGH_PRIORITY,
                GeneralSmscConstants.MEDIUM_PRIORITY,
                GeneralSmscConstants.LOW_PRIORITY
        );
    }


    @DisplayName("When NoAvailableSessionException And AutoRetryConfigured Then Send To Retries Topic")
    @ParameterizedTest
    @MethodSource("priorityProvider")
    void handleExceptionForErrorOnSendSubmitSmWhenNoAvailableSessionThenRetries(String priority) throws Exception {
        lenient().when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");

        Gateway gateway = Gateway.builder()
                .networkId(1)
                .systemId("op_01_smpp_gw")
                .password("1234")
                .mno(1)
                .tps(1)
                .protocol("SMPP")
                .autoRetryErrorCode(String.valueOf(ErrorCodes.SMPP_CONNECTION_UNAVAILABLE))
                .noRetryErrorCode("")
                .retryAlternateDestinationErrorCode("")
                .build();

        SmppConnectionManager smppConnectionManager = new SmppConnectionManager(
                redisManager,
                gateway,
                socketSession,
                errorCodeMappingConcurrentHashMap,
                appProperties,
                scyllaManager,
                kafkaTemplate,
                kafkaConsumerFactory
        );

        MessageEvent submitSmEvent = new MessageEvent();
        submitSmEvent.setId(System.currentTimeMillis() + "-" + System.nanoTime());
        submitSmEvent.setMessageId("msg-123");
        submitSmEvent.setValidityPeriod(60);
        submitSmEvent.setLastRetry(false);
        submitSmEvent.setSmscMessagePriority(priority);

        // Simulate missing SMPP session (trigger NoAvailableSessionException)
        NoAvailableSessionException exception =
                new NoAvailableSessionException("No active SMPP sessions for network");

        Method handleExMethod = SmppConnectionManager.class
                .getDeclaredMethod("handleExceptionForErrorOnSendSubmitSm", MessageEvent.class, Exception.class);
        handleExMethod.setAccessible(true);

        handleExMethod.invoke(smppConnectionManager, submitSmEvent, exception);

        ArgumentCaptor<String> cdrMessageCapture = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CDR_TOPIC), cdrMessageCapture.capture());

        String cdrInRaw = cdrMessageCapture.getValue();
        UtilsRecords.Cdr cdr = Converter.stringToObject(cdrInRaw, UtilsRecords.Cdr.class);
        assertEquals(ErrorCodes.SMPP_CONNECTION_UNAVAILABLE + "", cdr.statusCode());
        // Send Message Event to Retry and no Error Info
        assertEquals(1, submitSmEvent.getRetryNumber());
        assertNull(submitSmEvent.getErrorCode());

        switch (priority) {
            case GeneralSmscConstants.HIGH_PRIORITY ->
                    verify(kafkaTemplate).send(KafkaTopicsConstants.RETRIES_HIGH_TOPIC, submitSmEvent.toString());
            case GeneralSmscConstants.MEDIUM_PRIORITY ->
                    verify(kafkaTemplate).send(KafkaTopicsConstants.RETRIES_MEDIUM_TOPIC, submitSmEvent.toString());
            case GeneralSmscConstants.LOW_PRIORITY ->
                    verify(kafkaTemplate).send(KafkaTopicsConstants.RETRIES_LOW_TOPIC, submitSmEvent.toString());
            default -> fail("Unexpected priority: " + priority);
        }

        // Access private session list to force internal states
        Field sessionsField = SmppConnectionManager.class.getDeclaredField("sessions");
        sessionsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<SMPPSession> sessions = (List<SMPPSession>) sessionsField.get(smppConnectionManager);

        // Validate getRandomSession() throws when selectedSession == null
        sessions.clear();
        sessions.add(null);

        Method getRandomSessionMethod = SmppConnectionManager.class
                .getDeclaredMethod("getRandomSession");
        getRandomSessionMethod.setAccessible(true);

        try {
            getRandomSessionMethod.invoke(smppConnectionManager);
            fail("Expected NoAvailableSessionException when selectedSession is null");
        } catch (InvocationTargetException ex) {
            assertInstanceOf(NoAvailableSessionException.class, ex.getCause());
            assertEquals(
                    "Selected session is not bound for networkId = " + gateway.getNetworkId(),
                    ex.getCause().getMessage()
            );
        }

        // Validate getRandomSession() throws when session is not bound
        sessions.clear();
        SMPPSession unboundSession = new SMPPSession();
        sessions.add(unboundSession);

        try {
            getRandomSessionMethod.invoke(smppConnectionManager);
            fail("Expected NoAvailableSessionException when session is not bound");
        } catch (InvocationTargetException ex) {
            assertInstanceOf(NoAvailableSessionException.class, ex.getCause());
            assertEquals(
                    "Selected session is not bound for networkId = " + gateway.getNetworkId(),
                    ex.getCause().getMessage()
            );
        }
    }

    @Test
    @DisplayName("When connect is called then retry is scheduled using gateway bindRetryPeriod")
    void connectWhenCalledThenRetryIsScheduled() throws Exception {
        Gateway gateway = Gateway.builder()
                .networkId(5)
                .systemId("op_01_smpp_gw")
                .password("1234")
                .protocol("SMPP")
                .ip("192.168.1.100")
                .port(2775)
                .sessionsNumber(1)
                .status("stopped")
                .enabled(1)
                .tps(10)
                .mno(1)
                .bindRetryPeriod(30000)
                .build();

        lenient().when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        SmppConnectionManager manager = new SmppConnectionManager(
                redisManager, gateway, socketSession,
                errorCodeMappingConcurrentHashMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory
        );

        Field schedulerField = SmppConnectionManager.class.getDeclaredField("schedulerRetryBind");
        schedulerField.setAccessible(true);
        ScheduledExecutorService schedulerMock = mock(ScheduledExecutorService.class);
        schedulerField.set(manager, schedulerMock);

        manager.connect();
        verify(schedulerMock).scheduleWithFixedDelay(
                any(Runnable.class),
                eq(30000L), //initial delay
                eq(30000L), //delay
                eq(TimeUnit.MILLISECONDS)
        );
    }

    @Test
    @DisplayName("An error on send publishes an ECUR charging completion")
    void handleExceptionForErrorOnSendSubmitSmWhenEcurThenPublishesFailedChargingCompletion() throws Exception {
        Gateway gateway = Gateway.builder()
                .networkId(1)
                .systemId("smpp-gateway")
                .mno(1)
                .protocol("SMPP")
                .build();
        MessageEvent messageEvent = new MessageEvent();
        messageEvent.setId("1719421854353-1");
        messageEvent.setMessageId("charging-reservation-id");
        messageEvent.setChargingType(ChargingType.ECUR);
        messageEvent.setSmscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY);
        messageEvent.setRegisteredDelivery(RequestDelivery.NON_REQUEST_DLR.getValue());

        SmppConnectionManager manager = new SmppConnectionManager(redisManager, gateway, socketSession,
                errorCodeMappingConcurrentHashMap, appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        Method handleExceptionForErrorOnSendSubmitSm = SmppConnectionManager.class
                .getDeclaredMethod("handleExceptionForErrorOnSendSubmitSm", MessageEvent.class, Exception.class);
        handleExceptionForErrorOnSendSubmitSm.setAccessible(true);
        handleExceptionForErrorOnSendSubmitSm.invoke(manager, messageEvent,
                new NoAvailableSessionException("no session"));

        ArgumentCaptor<String> chargingCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CHARGING_COMPLETION_TOPIC), chargingCaptor.capture());
        MessageEvent completionEvent = Converter.stringToObject(chargingCaptor.getValue(), MessageEvent.class);
        assertEquals("charging-reservation-id", completionEvent.getMessageId());
        assertNull(completionEvent.getStatus());
        assertEquals(Map.of(ChargingUtils.CHARGING_SUCCESS_PARAM, false), completionEvent.getCustomParams());
        assertEquals(ErrorCodes.SMPP_CONNECTION_UNAVAILABLE, completionEvent.getErrorCode());
    }
}
