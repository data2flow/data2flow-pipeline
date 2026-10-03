package net.java21.data2flow.pipeline.common;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** 원본 payload 보기 형식({@code raw_messages.payload_encoding}, API-ING-06): JSON, TEXT, BINARY */
public enum PayloadEncoding {
    JSON, TEXT, BINARY;

    /** UTF-8로 읽히고 {·[로 시작하면 JSON, 읽히고 제어 문자가 없으면 TEXT, 아니면 BINARY */
    public static PayloadEncoding detect(byte[] payload) {
        String text = utf8(payload);
        if (text == null) {
            return BINARY;
        }
        String trimmed = text.strip();
        if (!trimmed.isEmpty() && (trimmed.charAt(0) == '{' || trimmed.charAt(0) == '[')) {
            return JSON;
        }
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isISOControl(c) && c != '\n' && c != '\r' && c != '\t') {
                return BINARY;
            }
        }
        return TEXT;
    }

    /** 엄격한 UTF-8 해석. 잘못된 바이트가 있으면 null */
    public static String utf8(byte[] payload) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(payload)).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }
}
