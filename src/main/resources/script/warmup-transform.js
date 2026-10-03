// 예열용 TRANSFORM(ScriptSandbox.warmUp)
class Acc { constructor() { this.n = 0; } add(v) { this.n += v; return this; } }
function transform(msg, ctx) {
    const {util, config, device, last} = ctx;
    const t = util.metric(msg, 'temperature');
    const h = util.metric(msg, 'humidity');
    if (t && h) {
        util.setMetric(msg, 'dew_point', util.dewPoint(t.value, h.value), '℃');
        util.setMetric(msg, 'thi', util.thi(t.value, h.value));
        t.value = util.round(util.clamp(t.value + (config.offset || 0) + (device?.attributes?.tempOffset ?? 0), -40, 85), 1);
    }
    const values = msg.metrics.map(m => m.value).filter(v => typeof v === 'number').sort((a, b) => a - b);
    const set = new Set(msg.metrics.map(m => m.key));
    const map = new Map(Object.entries({a: 1, b: 2}));
    const acc = values.reduce((a, v) => a.add(v), new Acc());
    const re = /^([a-z]+)_?(\d*)$/i.exec('dew_point') || [];
    msg.meta = Object.assign({}, msg.meta, {
        n: acc.n.toFixed(2), keys: [...set].slice(0, 3), m: map.size, re: re.length, includes: set.has('thi'),
        idx: values.indexOf(values[0]), pad: String(last.temperature ? last.temperature.value : 0).padStart(4, '0'),
        f: util.c2f(20), p: util.convert(1013, 'hPa', 'kPa'), abs: util.absHumidity(20, 50), now: util.now(),
        d: Math.max(...values, 0) + Math.min(0, ...values), j: JSON.parse(JSON.stringify({x: [1, 2]})).x.length,
        s: `${msg.deviceId}`.replace(/1/g, 'x'), arr: Array.from([1, 2, 3], x => x * 2).includes(4)
    });
    util.removeMetric(msg, 'thi');
    try { null.x; } catch (e) { msg.meta.err = e instanceof TypeError; }
    return msg;
}
