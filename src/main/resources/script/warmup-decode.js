// 예열용 DECODE(ScriptSandbox.warmUp): 바이트 헬퍼와 자주 쓰는 내장 함수 경로를 한 번씩 지난다
function decode(input, ctx) {
    const b = ctx.util.bytes.fromBase64(input.payloadBase64);
    const hex = ctx.util.bytes.toHex(b);
    const back = ctx.util.bytes.fromHex(hex);
    const metrics = [];
    for (let i = 0; i + 1 < back.length;) {
        const channel = back[i], type = back[i + 1];
        if (channel === 0x01 && type === 0x75) { metrics.push({key: 'battery', value: ctx.util.bytes.readUInt8(back, i + 2)}); i += 3; }
        else if (channel === 0x03 && type === 0x67) { metrics.push({key: 'temperature', value: ctx.util.bytes.readInt16LE(back, i + 2) / 10, unit: '℃'}); i += 4; }
        else if (channel === 0x04 && type === 0x68) { metrics.push({key: 'humidity', value: ctx.util.bytes.readUInt8(back, i + 2) / 2}); i += 3; }
        else if (channel === 0x05 && type === 0x7d) { metrics.push({key: 'co2', value: ctx.util.bytes.readUInt16LE(back, i + 2)}); i += 4; }
        else { break; }
    }
    const view = new DataView(new Uint8Array([1, 2, 3, 4]).buffer);
    const tags = Object.assign({}, {n: String(view.getUint16(0, true))});
    const text = JSON.stringify({m: metrics.map(m => m.key).join(','), t: (input.topic || '').split('/').length});
    console.log('decode', text, input.payload && input.payload.a);
    return {externalId: 'warm-' + hex.substring(0, 4).toLowerCase(), measuredAt: new Date(input.receivedAt).toISOString(),
        metrics: metrics.filter(m => Number.isFinite(m.value)), link: {rssi: -70, snr: 9.5, frameCounter: 1}, meta: {tags: tags}};
}
