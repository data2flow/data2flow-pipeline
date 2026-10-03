package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.pipeline.script.domain.ScriptOutcome;
import net.java21.data2flow.pipeline.support.ScriptSandboxHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/** SCR-01.04 TC-SCR-011 · AT-SCR-05.1: ctx.util 헬퍼(SCR-api §3.3) */
class ScriptHelpersTest {

    private static double eval(String expression) {
        ScriptOutcome outcome = ScriptSandboxHarness.transform(
                "function transform(msg, ctx) { const u = ctx.util; return {r: " + expression + "}; }");
        assertThat(outcome.ok()).as(expression + " → " + outcome.failure()).isTrue();
        return outcome.output().get("r").asDouble();
    }

    @ParameterizedTest(name = "{0} = {1}")
    @CsvSource(delimiter = '|', value = {
            "u.dewPoint(25, 60)|16.7",
            "u.thi(28, 70)|78.4",
            "u.round(1.005, 2)|1.01",
            "u.round(2.345, 2)|2.35",
            "u.round(-1.5)|-1",
            "u.clamp(85, -20, 60)|60",
            "u.clamp(-30, -20, 60)|-20",
            "u.cToF(20)|68",
            "u.c2f(100)|212",
            "u.f2c(212)|100",
            "u.convert(1013, 'hPa', 'kPa')|101.3",
            "u.convert(25, 'C', 'K')|298.15",
            "u.convert(5, 'm', 'm')|5",
            "u.absHumidity(20, 50)|8.64",
            "u.bytes.readUInt16LE(u.bytes.fromHex('3401'), 0)|308",
            "u.bytes.readUInt16BE(u.bytes.fromHex('0134'), 0)|308",
            "u.bytes.readInt16LE(u.bytes.fromHex('F6FF'), 0)|-10",
            "u.bytes.readInt16BE(u.bytes.fromHex('FFF6'), 0)|-10",
            "u.bytes.readUInt8(u.bytes.fromHex('FF'), 0)|255",
            "u.bytes.readInt8(u.bytes.fromHex('FF'), 0)|-1",
            "u.bytes.readUInt32LE(u.bytes.fromHex('01000080'), 0)|2147483649",
            "u.bytes.readUInt32BE(u.bytes.fromHex('80000001'), 0)|2147483649",
            "u.bytes.readInt32LE(u.bytes.fromHex('FFFFFFFF'), 0)|-1",
            "u.bytes.readInt32BE(u.bytes.fromHex('FFFFFFFE'), 0)|-2",
            "u.base64ToBytes('AXVoAQ==').length|4",
            "u.bytes.fromBase64(u.bytes.toBase64(u.bytes.fromHex('0102030405')))[4]|5",
            "u.bytes.fromBase64(u.bytes.toBase64(u.bytes.fromHex('0102')))[1]|2",
            "u.hexToBytes('0a0b').length|2",
            "u.movingAvg('t', 21.5, 3)|21.5",
            "u.metric(u.setMetric(msg, 'x', 3, 'u') && msg, 'x').value|3"})
    @DisplayName("[SCR-01.04][AT-SCR-05.1] TC-SCR-011 헬퍼 값")
    void helpers(String expression, double expected) {
        assertThat(eval(expression)).isCloseTo(expected, org.assertj.core.data.Offset.offset(0.005));
    }

    @Test
    @DisplayName("[SCR-01.04][AT-SCR-05.1] TC-SCR-011 범위 밖 offset은 RangeError, 16진수가 아니면 RangeError, bytesToHex 왕복, removeMetric·now")
    void errorsAndRoundTrips() {
        ScriptOutcome range = ScriptSandboxHarness.transform(
                "function transform(msg, ctx) { return {r: ctx.util.bytes.readUInt16LE(ctx.util.bytes.fromHex('01'), 0)}; }");
        assertThat(range.failure().message()).contains("RangeError");
        ScriptOutcome hex = ScriptSandboxHarness.transform(
                "function transform(msg, ctx) { return {r: ctx.util.bytes.fromHex('zz')}; }");
        assertThat(hex.failure().message()).contains("RangeError");
        ScriptOutcome unit = ScriptSandboxHarness.transform(
                "function transform(msg, ctx) { return {r: ctx.util.convert(1, 'C', 'parsec')}; }");
        assertThat(unit.ok()).isFalse();
        ScriptOutcome ok = ScriptSandboxHarness.transform("""
                function transform(msg, ctx) {
                  const u = ctx.util;
                  u.setMetric(msg, 'temperature', 30);
                  u.removeMetric(msg, 'missing');
                  return {hex: u.bytesToHex(u.bytes.fromHex('0aff')), now: u.now(), t: u.metric(msg, 'temperature').value,
                          none: u.metric(msg, 'none'), keys: msg.metrics.length, rounded: u.round('x')};
                }
                """);
        assertThat(ok.output().get("hex").asString()).isEqualTo("0aff");
        assertThat(ok.output().get("now").asString()).isEqualTo("2026-10-03T00:00:00Z");
        assertThat(ok.output().get("t").asDouble()).isEqualTo(30);
        assertThat(ok.output().get("none").isNull()).isTrue();
        assertThat(ok.output().get("keys").asInt()).isEqualTo(1);
    }
}
