package com.paicbd.module.smpp;

import com.paicbd.module.utils.AppProperties;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.MessagePart;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.RedisManager;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.ws.SocketSession;
import org.jsmpp.PDUException;
import org.jsmpp.bean.DataCoding;
import org.jsmpp.bean.ESMClass;
import org.jsmpp.bean.NumberingPlanIndicator;
import org.jsmpp.bean.OptionalParameter;
import org.jsmpp.bean.RegisteredDelivery;
import org.jsmpp.bean.TypeOfNumber;
import org.jsmpp.extra.SessionState;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.session.SubmitSmResult;
import org.jsmpp.util.MessageId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.KafkaTemplate;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SmppConnectionManagerMultipartTest {

    private static final int TOTAL_PARTS = 4;

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
    @Mock
    SMPPSession smppSession;

    private SmppConnectionManager manager;

    @BeforeEach
    void setUp() throws Exception {
        buildManager("", "", "504");
    }

    private void buildManager(String noRetry, String alternateDestination, String autoRetry) throws Exception {
        ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingMap = new ConcurrentHashMap<>();
        Gateway gateway = Gateway.builder()
                .networkId(1).name("SMPP-Operator").systemId("op_01_smpp_gw")
                .ip("127.0.0.1").port(2779).bindType("TRANSCEIVER")
                .sessionsNumber(1).mno(1).protocol("SMPP").status("STARTED").enabled(0)
                .noRetryErrorCode(noRetry).retryAlternateDestinationErrorCode(alternateDestination)
                .autoRetryErrorCode(autoRetry)
                .build();
        manager = new SmppConnectionManager(
                redisManager, gateway, socketSession, errorCodeMappingMap,
                appProperties, scyllaManager, kafkaTemplate, kafkaConsumerFactory);

        when(smppSession.getSessionState()).thenReturn(SessionState.BOUND_TRX);
        Field sessionsField = SmppConnectionManager.class.getDeclaredField("sessions");
        sessionsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<SMPPSession> sessions = (List<SMPPSession>) sessionsField.get(manager);
        sessions.add(smppSession);
    }

    private List<String> cdrsPublished() {
        ArgumentCaptor<String> cdrCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, atLeast(0)).send(eq(KafkaTopicsConstants.CDR_TOPIC), cdrCaptor.capture());
        return cdrCaptor.getAllValues();
    }

    private MessageEvent buildMultipartEvent(boolean retryAllParts) {
        List<MessagePart> parts = new ArrayList<>();
        for (int sequence = 1; sequence <= TOTAL_PARTS; sequence++) {
            MessagePart part = new MessagePart();
            part.setMessageId("mid-part-" + sequence);
            part.setShortMessage("part-" + sequence);
            part.setMsgReferenceNumber("77");
            part.setSegmentSequence(sequence);
            part.setTotalSegment(TOTAL_PARTS);
            part.setPartBytes(("part-" + sequence).getBytes());
            parts.add(part);
        }

        return MessageEvent.builder()
                .id(System.currentTimeMillis() + "-1")
                .messageId("mid-parent")
                .parentId("mid-parent")
                .systemId("sp_test")
                .originProtocol("SMPP").originNetworkType("SP").originNetworkId(10)
                .destProtocol("SMPP").destNetworkType("GW").destNetworkId(1)
                .serviceType("").scheduleDeliveryTime("").stringValidityPeriod("")
                .sourceAddr("50588888888").sourceAddrTon(1).sourceAddrNpi(1)
                .destinationAddr("50511112222").destAddrTon(1).destAddrNpi(1)
                .shortMessage("full message")
                .dataCoding(0)
                .esmClass(0)
                .validityPeriod(60)
                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                .smscMessagePriority("MEDIUM")
                .splitForSmsc(true)
                .retryAllParts(retryAllParts)
                .messageParts(parts)
                .build();
    }

    private void stubSubmitFailingAtSegment(int failingSegment) throws Exception {
        when(smppSession.submitShortMessage(nullable(String.class), any(TypeOfNumber.class), any(NumberingPlanIndicator.class),
                nullable(String.class), any(TypeOfNumber.class), any(NumberingPlanIndicator.class), nullable(String.class),
                any(ESMClass.class), anyByte(), anyByte(), nullable(String.class), nullable(String.class),
                any(RegisteredDelivery.class), anyByte(), any(DataCoding.class), anyByte(), any(byte[].class),
                any(OptionalParameter[].class)))
                .thenAnswer(invocation -> {
                    byte[] payload = invocation.getArgument(16);
                    String content = new String(payload);
                    if (content.equals("part-" + failingSegment)) {
                        throw new PDUException("simulated failure on segment " + failingSegment);
                    }
                    return new SubmitSmResult(new MessageId("srv-" + content), new OptionalParameter[0]);
                });
    }

    private void invokeSendMultipart(MessageEvent event) throws Exception {
        Method method = SmppConnectionManager.class.getDeclaredMethod("sendMultipartMessage", MessageEvent.class);
        method.setAccessible(true);
        method.invoke(manager, event);
    }

    private int submitCallCount() throws Exception {
        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(smppSession, atLeast(0)).submitShortMessage(nullable(String.class),
                any(TypeOfNumber.class), any(NumberingPlanIndicator.class), nullable(String.class),
                any(TypeOfNumber.class), any(NumberingPlanIndicator.class), nullable(String.class),
                any(ESMClass.class), anyByte(), anyByte(), nullable(String.class), nullable(String.class),
                any(RegisteredDelivery.class), anyByte(), any(DataCoding.class), anyByte(),
                payloadCaptor.capture(), any(OptionalParameter[].class));
        return payloadCaptor.getAllValues().size();
    }

    @Test
    @DisplayName("all parts delivered in order when every submit succeeds")
    void allPartsDeliveredWhenEverySubmitSucceeds() throws Exception {
        stubSubmitFailingAtSegment(0);
        MessageEvent event = buildMultipartEvent(true);

        invokeSendMultipart(event);

        assertEquals(TOTAL_PARTS, submitCallCount());
        assertTrue(event.getMessageParts().stream().allMatch(MessagePart::isSent));
        assertTrue(event.getPendingMessageParts().isEmpty());
        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.RETRIES_MEDIUM_TOPIC), anyString());
    }

    @Test
    @DisplayName("transmission stops at the failing part and later parts are not sent")
    void transmissionStopsAtFailingPart() throws Exception {
        stubSubmitFailingAtSegment(3);
        MessageEvent event = buildMultipartEvent(true);

        invokeSendMultipart(event);

        assertEquals(3, submitCallCount());
        List<MessagePart> parts = event.getMessageParts();
        assertTrue(parts.get(0).isSent());
        assertTrue(parts.get(1).isSent());
        assertFalse(parts.get(2).isSent());
        assertFalse(parts.get(3).isSent());
    }

    @Test
    @DisplayName("failure publishes a single retry event carrying the parent and its part status")
    void failurePublishesSingleRetryEventWithParent() throws Exception {
        stubSubmitFailingAtSegment(3);
        MessageEvent event = buildMultipartEvent(false);

        invokeSendMultipart(event);

        ArgumentCaptor<String> retryCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(1)).send(eq(KafkaTopicsConstants.RETRIES_MEDIUM_TOPIC), retryCaptor.capture());

        MessageEvent retried = Converter.stringToObject(retryCaptor.getValue(), MessageEvent.class);
        assertEquals("mid-parent", retried.getMessageId());
        assertEquals(TOTAL_PARTS, retried.getMessageParts().size());
        assertEquals(2, retried.getPendingMessageParts().size());
        assertEquals(3, retried.getPendingMessageParts().get(0).getSegmentSequence());
    }

    @Test
    @DisplayName("a failed part publishes one CDR carrying its own segment, not a second one for the parent")
    void failedPartPublishesSingleCdrForThatSegment() throws Exception {
        stubSubmitFailingAtSegment(3);
        MessageEvent event = buildMultipartEvent(false);

        invokeSendMultipart(event);

        ArgumentCaptor<String> cdrCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(3)).send(eq(KafkaTopicsConstants.CDR_TOPIC), cdrCaptor.capture());

        List<String> publishedCdrs = cdrCaptor.getAllValues();
        assertTrue(publishedCdrs.get(2).contains("\"segment_sequence\":\"3\""));
        assertTrue(publishedCdrs.get(2).contains("\"status\":\"FAILED\""));
        assertEquals(0, publishedCdrs.stream().filter(cdrRecord -> cdrRecord.contains("\"message\":\"full message\"")).count());
    }

    @Test
    @DisplayName("a part without segment sequence is skipped and does not break the transmission")
    void partWithoutSegmentSequenceIsSkipped() throws Exception {
        stubSubmitFailingAtSegment(0);
        MessageEvent event = buildMultipartEvent(false);
        event.getMessageParts().get(1).setSegmentSequence(null);

        invokeSendMultipart(event);

        assertEquals(TOTAL_PARTS - 1, submitCallCount());
    }

    @Test
    @DisplayName("a validity period of zero finishes the message instead of retrying it")
    void zeroValidityPeriodFinishesWithoutRetry() throws Exception {
        stubSubmitFailingAtSegment(2);
        MessageEvent event = buildMultipartEvent(false);
        event.setValidityPeriod(0);

        invokeSendMultipart(event);

        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.RETRIES_MEDIUM_TOPIC), anyString());
        verify(kafkaTemplate, times(1)).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());

        List<String> publishedCdrs = cdrsPublished();
        assertEquals(2, publishedCdrs.size());
        assertTrue(publishedCdrs.get(1).contains("\"segment_sequence\":\"2\""));
    }

    @Test
    @DisplayName("an error listed as no retry fails the message and still reports the failed segment")
    void noRetryErrorCodeFailsImmediately() throws Exception {
        buildManager("504", "", "");
        stubSubmitFailingAtSegment(2);
        MessageEvent event = buildMultipartEvent(false);

        invokeSendMultipart(event);

        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.RETRIES_MEDIUM_TOPIC), anyString());
        verify(kafkaTemplate, times(1)).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());

        List<String> publishedCdrs = cdrsPublished();
        assertEquals(2, publishedCdrs.size());
        assertTrue(publishedCdrs.get(1).contains("\"segment_sequence\":\"2\""));
        assertTrue(publishedCdrs.get(1).contains("\"status\":\"FAILED\""));
    }

    @Test
    @DisplayName("an error listed as alternate destination republishes the parent with its part status")
    void alternateDestinationRepublishesParent() throws Exception {
        buildManager("", "504", "");
        stubSubmitFailingAtSegment(2);
        MessageEvent event = buildMultipartEvent(false);

        invokeSendMultipart(event);

        ArgumentCaptor<String> routingCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(1)).send(eq(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC), routingCaptor.capture());

        MessageEvent republished = Converter.stringToObject(routingCaptor.getValue(), MessageEvent.class);
        assertEquals("mid-parent", republished.getMessageId());
        assertEquals(TOTAL_PARTS, republished.getMessageParts().size());
        assertTrue(republished.getMessageParts().get(0).isSent());
        assertFalse(republished.getMessageParts().get(1).isSent());

        List<String> publishedCdrs = cdrsPublished();
        assertEquals(2, publishedCdrs.size());
        assertTrue(publishedCdrs.get(1).contains("\"segment_sequence\":\"2\""));
    }

    @Test
    @DisplayName("an error not listed in any retry group fails the message without a retry")
    void errorNotListedAnywhereFailsTheMessage() throws Exception {
        buildManager("", "", "");
        stubSubmitFailingAtSegment(2);
        MessageEvent event = buildMultipartEvent(false);

        invokeSendMultipart(event);

        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.RETRIES_MEDIUM_TOPIC), anyString());
        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC), anyString());
        verify(kafkaTemplate, times(1)).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());

        List<String> publishedCdrs = cdrsPublished();
        assertEquals(2, publishedCdrs.size());
        assertTrue(publishedCdrs.get(1).contains("\"segment_sequence\":\"2\""));
    }

    @Test
    @DisplayName("a message marked for refund is refunded when the multipart transaction ends in failure")
    void failedMultipartAppliesTheRefund() throws Exception {
        buildManager("", "", "");
        stubSubmitFailingAtSegment(2);
        MessageEvent event = buildMultipartEvent(false);
        event.setApplyForRefund(true);

        invokeSendMultipart(event);

        ArgumentCaptor<String> chargingCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(1)).send(eq(KafkaTopicsConstants.CHARGING_MEDIUM_MESSAGE_TOPIC),
                chargingCaptor.capture());

        MessageEvent refunded = Converter.stringToObject(chargingCaptor.getValue(), MessageEvent.class);
        assertEquals("mid-parent", refunded.getMessageId());
        assertTrue(refunded.isReadyForRefund());
    }

    @Test
    @DisplayName("only the parent of a split message is treated as the parent, never one of its parts")
    void onlyTheParentOfASplitMessageIsTheParent() throws Exception {
        Method isSplitParent = SmppConnectionManager.class.getDeclaredMethod("isSplitParent", MessageEvent.class);
        isSplitParent.setAccessible(true);

        MessageEvent parent = buildMultipartEvent(false);
        assertEquals(Boolean.TRUE, isSplitParent.invoke(null, parent));

        MessageEvent part = Converter.deepCopy(parent, MessageEvent.class);
        part.setSegmentSequence(2);
        assertEquals(Boolean.FALSE, isSplitParent.invoke(null, part));

        MessageEvent single = Converter.deepCopy(parent, MessageEvent.class);
        single.setMessageParts(null);
        assertEquals(Boolean.FALSE, isSplitParent.invoke(null, single));
    }

    @Test
    @DisplayName("an event without parts, null or empty, does not take the multipart path")
    void eventWithoutPartsIsNotMultipart() throws Exception {
        Method hasMessageParts = SmppConnectionManager.class.getDeclaredMethod("hasMessageParts", MessageEvent.class);
        hasMessageParts.setAccessible(true);

        MessageEvent event = buildMultipartEvent(false);
        assertEquals(Boolean.TRUE, hasMessageParts.invoke(null, event));

        event.setMessageParts(new ArrayList<>());
        assertEquals(Boolean.FALSE, hasMessageParts.invoke(null, event));

        event.setMessageParts(null);
        assertEquals(Boolean.FALSE, hasMessageParts.invoke(null, event));
    }

    @Test
    @DisplayName("no skipped CDR is published when every part was already delivered")
    void noSkippedCdrWhenEveryPartWasDelivered() throws Exception {
        MessageEvent event = buildMultipartEvent(false);
        event.setSplitForSmsc(false);
        event.getMessageParts().forEach(part -> part.setSent(true));

        Method publishSkipped = SmppConnectionManager.class
                .getDeclaredMethod("publishSkippedPartsCdr", MessageEvent.class);
        publishSkipped.setAccessible(true);
        publishSkipped.invoke(manager, event);

        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.CDR_TOPIC), anyString());
    }

    @Test
    @DisplayName("parts never attempted are reported as skipped when the message was split by the ESME")
    void unsentPartsAreReportedAsSkippedWhenSplitByEsme() throws Exception {
        stubSubmitFailingAtSegment(2);
        MessageEvent event = buildMultipartEvent(false);
        event.setSplitForSmsc(false);
        event.setValidityPeriod(0);

        invokeSendMultipart(event);

        List<String> publishedCdrs = cdrsPublished();
        assertEquals(TOTAL_PARTS, publishedCdrs.size());
        assertTrue(publishedCdrs.get(1).contains("\"segment_sequence\":\"2\""));
        assertTrue(publishedCdrs.get(2).contains("\"segment_sequence\":\"3\""));
        assertTrue(publishedCdrs.get(2).contains("\"status_code\":\"803\""));
        assertTrue(publishedCdrs.get(3).contains("\"segment_sequence\":\"4\""));
        assertTrue(publishedCdrs.get(3).contains("\"status_code\":\"803\""));
    }

    @Test
    @DisplayName("parts never attempted are not reported when the SMSC did the split and the property is off")
    void unsentPartsAreNotReportedWhenSplitBySmscAndPropertyIsOff() throws Exception {
        when(appProperties.isSmscGenerateCdrWhenSplitSmsc()).thenReturn(false);
        stubSubmitFailingAtSegment(2);
        MessageEvent event = buildMultipartEvent(false);
        event.setSplitForSmsc(true);
        event.setValidityPeriod(0);

        invokeSendMultipart(event);

        List<String> publishedCdrs = cdrsPublished();
        assertEquals(2, publishedCdrs.size());
        assertEquals(0, publishedCdrs.stream().filter(cdrRecord -> cdrRecord.contains("\"status_code\":\"803\"")).count());
    }

    @Test
    @DisplayName("parts never attempted are reported when the SMSC did the split and the property is on")
    void unsentPartsAreReportedWhenSplitBySmscAndPropertyIsOn() throws Exception {
        when(appProperties.isSmscGenerateCdrWhenSplitSmsc()).thenReturn(true);
        stubSubmitFailingAtSegment(2);
        MessageEvent event = buildMultipartEvent(false);
        event.setSplitForSmsc(true);
        event.setValidityPeriod(0);

        invokeSendMultipart(event);

        List<String> publishedCdrs = cdrsPublished();
        assertEquals(TOTAL_PARTS, publishedCdrs.size());
        assertEquals(2, publishedCdrs.stream().filter(cdrRecord -> cdrRecord.contains("\"status_code\":\"803\"")).count());
    }

    @Test
    @DisplayName("retry with retryAllParts disabled resends only the failed and pending parts, in order")
    void retryResumesFromFailedPartWhenRetryAllDisabled() throws Exception {
        stubSubmitFailingAtSegment(0);
        MessageEvent event = buildMultipartEvent(false);
        event.setRetry(true);
        event.getMessageParts().get(0).setSent(true);
        event.getMessageParts().get(1).setSent(true);

        invokeSendMultipart(event);

        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(smppSession, times(2)).submitShortMessage(nullable(String.class), any(TypeOfNumber.class),
                any(NumberingPlanIndicator.class), nullable(String.class), any(TypeOfNumber.class),
                any(NumberingPlanIndicator.class), nullable(String.class), any(ESMClass.class), anyByte(), anyByte(),
                nullable(String.class), nullable(String.class), any(RegisteredDelivery.class), anyByte(),
                any(DataCoding.class), anyByte(), payloadCaptor.capture(), any(OptionalParameter[].class));

        List<byte[]> payloads = payloadCaptor.getAllValues();
        assertEquals("part-3", new String(payloads.get(0)));
        assertEquals("part-4", new String(payloads.get(1)));
        assertTrue(event.getMessageParts().stream().allMatch(MessagePart::isSent));
    }

    @Test
    @DisplayName("retry with retryAllParts enabled resends every part from the first one")
    void retryResendsAllPartsWhenRetryAllEnabled() throws Exception {
        stubSubmitFailingAtSegment(0);
        MessageEvent event = buildMultipartEvent(true);
        event.setRetry(true);
        event.getMessageParts().get(0).setSent(true);
        event.getMessageParts().get(1).setSent(true);

        invokeSendMultipart(event);

        assertEquals(TOTAL_PARTS, submitCallCount());
        assertTrue(event.getMessageParts().stream().allMatch(MessagePart::isSent));
    }

    @Test
    @DisplayName("last retry emits the failed DLR once from the parent message")
    void lastRetryEmitsFailedDlrFromParent() throws Exception {
        stubSubmitFailingAtSegment(3);
        MessageEvent event = buildMultipartEvent(false);
        event.setLastRetry(true);

        invokeSendMultipart(event);

        ArgumentCaptor<String> dlrCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(1)).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), dlrCaptor.capture());

        MessageEvent deliverSmEvent = Converter.stringToObject(dlrCaptor.getValue(), MessageEvent.class);
        assertEquals("mid-parent", deliverSmEvent.getMessageId());
        verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.RETRIES_MEDIUM_TOPIC), anyString());
    }

    @Test
    @DisplayName("whole message is resent when it comes back for retry with every part already delivered")
    void wholeMessageIsResentWhenAllPartsAlreadyDelivered() throws Exception {
        stubSubmitFailingAtSegment(0);
        MessageEvent event = buildMultipartEvent(false);
        event.setRetry(true);
        event.getMessageParts().forEach(part -> part.setSent(true));

        invokeSendMultipart(event);

        assertEquals(TOTAL_PARTS, submitCallCount());
        assertTrue(event.getMessageParts().stream().allMatch(MessagePart::isSent));
    }

    @Test
    @DisplayName("parts are sent in segment order even when the incoming list is unordered")
    void partsAreSentInSegmentOrderWhenListIsUnordered() throws Exception {
        stubSubmitFailingAtSegment(0);
        MessageEvent event = buildMultipartEvent(true);
        List<MessagePart> shuffled = new ArrayList<>(event.getMessageParts());
        java.util.Collections.reverse(shuffled);
        event.setMessageParts(shuffled);

        invokeSendMultipart(event);

        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(smppSession, times(TOTAL_PARTS)).submitShortMessage(nullable(String.class), any(TypeOfNumber.class),
                any(NumberingPlanIndicator.class), nullable(String.class), any(TypeOfNumber.class),
                any(NumberingPlanIndicator.class), nullable(String.class), any(ESMClass.class), anyByte(), anyByte(),
                nullable(String.class), nullable(String.class), any(RegisteredDelivery.class), anyByte(),
                any(DataCoding.class), anyByte(), payloadCaptor.capture(), any(OptionalParameter[].class));

        List<byte[]> payloads = payloadCaptor.getAllValues();
        for (int index = 0; index < TOTAL_PARTS; index++) {
            assertEquals("part-" + (index + 1), new String(payloads.get(index)));
        }
    }
}
