package net.java21.data2flow.pipeline.device.domain;

/** 미등록 기기 처리 정책(ING-03.02, DSC {@code unknownDevicePolicy}). 기본 AUTO_REGISTER */
public enum UnknownDevicePolicy {
    AUTO_REGISTER, REJECT;

    public static UnknownDevicePolicy parse(String value) {
        return "REJECT".equalsIgnoreCase(value) ? REJECT : AUTO_REGISTER;
    }
}
