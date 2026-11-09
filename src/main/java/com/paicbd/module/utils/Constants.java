package com.paicbd.module.utils;

import lombok.Generated;

@Generated
public class Constants {
    private Constants() {
        throw new IllegalStateException("Utility class");
    }

    public static final String STOPPED = "STOPPED";
    public static final String BINDING = "BINDING";
    public static final String BOUND = "BOUND";
    public static final String UNBINDING = "UNBINDING";
    public static final String UNBOUND = "UNBOUND";
    public static final String UPDATE_GATEWAY_ENDPOINT = "/app/updateGateway";
    public static final String CONNECT_GATEWAY_ENDPOINT = "/app/connectGateway";
    public static final String RESPONSE_SMPP_CLIENT_ENDPOINT = "/app/response-smpp-client";
    public static final String STOP_GATEWAY_ENDPOINT = "/app/stopGateway";
    public static final String DELETE_GATEWAY_ENDPOINT = "/app/deleteGateway";
    public static final String UPDATE_ERROR_CODE_MAPPING_ENDPOINT = "/app/updateErrorCodeMapping"; // Receive mno_id as String
    public static final String PARAM_UPDATE_STATUS = "status";
    public static final String PARAM_UPDATE_SESSIONS = "sessions";
    public static final String ORIGIN_GATEWAY_TYPE = "GW";

    public static final String CDR_COMMENT = "cdr_comment";

    public static final String SKIPPED_PART_COMMENT = "Message part skipped due to previous part failure";
}
