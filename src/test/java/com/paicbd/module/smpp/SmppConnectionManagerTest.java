package com.paicbd.module.smpp;

import com.paicbd.module.utils.AppProperties;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.RedisManager;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.ws.SocketSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.jsmpp.session.SMPPSession;
import org.springframework.kafka.core.KafkaTemplate;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SmppConnectionManagerTest {

    @Mock
    RedisManager redisManager;
    @Mock
    SocketSession socketSession;
    @Mock
    AppProperties appProperties;
    @Mock
    ScyllaManager scyllaManager;
    @Mock
    KafkaTemplate<String, String> kafkaTemplate;
    @Mock
    KafkaConsumerFactory kafkaConsumerFactory;

    private Gateway buildGateway() {
        return Gateway.builder()
                .networkId(1).name("SMPP-Operator").systemId("op_01_smpp_gw")
                .ip("127.0.0.1").port(2779).bindType("TRANSCEIVER")
                .sessionsNumber(1).mno(1).protocol("SMPP").status("STARTED").enabled(0)
                .build();
    }

    @Test
    @DisplayName("failed DLR carries the CDR error description resolved from the original error code")
    void sendDeliverSmForFailedCasesAttachesCdrErrorDescription() throws Exception {
        ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingMap = new ConcurrentHashMap<>();
        SmppConnectionManager manager = new SmppConnectionManager(
                redisManager, buildGateway(), socketSession, errorCodeMappingMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory);

        MessageEvent submitSmEvent = MessageEvent.builder()
                .id(System.currentTimeMillis() + "-1")
                .messageId("mid-smpp-1")
                .sourceAddr("50588888888").sourceAddrTon(1).sourceAddrNpi(1)
                .destinationAddr("50511112222").destAddrTon(1).destAddrNpi(1)
                .shortMessage("Hello")
                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                .errorCode(500)
                .build();

        Method method = SmppConnectionManager.class.getDeclaredMethod(
                "sendDeliverSmForFailedCases", MessageEvent.class, int.class);
        method.setAccessible(true);
        method.invoke(manager, submitSmEvent, 500);

        ArgumentCaptor<String> dlrCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), dlrCaptor.capture());
        MessageEvent deliverSmEvent = Converter.stringToObject(dlrCaptor.getValue(), MessageEvent.class);
        assertEquals("Module Error", deliverSmEvent.getFromCustomParam("cdr_comment", null));
    }

    @Test
    @DisplayName("cleanAndUpperString trims trailing terminator and whitespace")
    void cleanAndUpperStringTrimsTerminatorAndWhitespace() {
        assertEquals("A5BDF10C", SmppConnectionManager.cleanAndUpperString("A5BDF10C "));

        String withNulTerminator = "a5bdf10c" + (char) 0;
        assertEquals("A5BDF10C", SmppConnectionManager.cleanAndUpperString(withNulTerminator));

        assertEquals("A5BDF10C", SmppConnectionManager.cleanAndUpperString("0000a5bdf10c"));
    }

    @Test
    @DisplayName("kafka consumers are re-created when a new session binds after the last one was closed")
    void consumersAreRecreatedAfterRebind() throws Exception {
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        Gateway gateway = buildGateway();
        gateway.setMessagesPerSecond(10);
        gateway.setMessagesPerSecondHigh(10);
        gateway.setPduProcessorDegree(1);
        SmppConnectionManager manager = new SmppConnectionManager(
                redisManager, gateway, socketSession, new ConcurrentHashMap<>(),
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory);

        Method checkAndManageConsumerState = SmppConnectionManager.class.getDeclaredMethod("checkAndManageConsumerState");
        checkAndManageConsumerState.setAccessible(true);
        Field kafkaConsumerHigh = SmppConnectionManager.class.getDeclaredField("kafkaConsumerHigh");
        kafkaConsumerHigh.setAccessible(true);

        manager.getSessions().add(mock(SMPPSession.class));
        checkAndManageConsumerState.invoke(manager);
        assertNotNull(kafkaConsumerHigh.get(manager));

        // enquire_link timeout: the only session is closed
        manager.getSessions().clear();
        checkAndManageConsumerState.invoke(manager);
        assertNull(kafkaConsumerHigh.get(manager));

        // rebind
        manager.getSessions().add(mock(SMPPSession.class));
        checkAndManageConsumerState.invoke(manager);
        assertNotNull(kafkaConsumerHigh.get(manager), "consumer must be re-created after rebind");
        verify(kafkaConsumerFactory, times(2)).createConsumer(any(), anyString());

        manager.getSessions().clear();
        checkAndManageConsumerState.invoke(manager);
    }
}
