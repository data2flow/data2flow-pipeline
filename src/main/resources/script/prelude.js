// data2flow 스크립트 샌드박스 머리말(SCR-02.01, design/flow-engine-and-live-reload.md §5).
// 실행마다 새 Context에서 사용자 코드보다 먼저 한 번 실행된다. 결과로 호스트가 쓸 함수 묶음을 돌려주므로 전역에는 아무것도 남기지 않는다
// (금지 API 막이만 전역에 둔다). 호스트는 이 묶음을 사용자 코드 실행 전에 잡아 두므로 사용자가 JSON·Object 등을 바꿔도 영향이 없다.
(function (g) {
    'use strict';

    // ---- 금지 API: 접근하면 SCRIPT_FORBIDDEN_API로 분류되는 오류를 던진다(BR-SCR-01, BR-SCR-05) ----
    const FORBIDDEN = ['require', 'fetch', 'XMLHttpRequest', 'WebSocket', 'setTimeout', 'setInterval', 'setImmediate',
        'clearTimeout', 'clearInterval', 'queueMicrotask', 'process', 'Java', 'Packages', 'Polyglot', 'Graal', 'load',
        'loadWithNewGlobal', 'print', 'printErr', 'quit', 'exit', 'Worker', 'WebAssembly', 'importScripts', 'java',
        'javax', 'javafx', 'com', 'org', 'edu', 'SharedArrayBuffer', 'Atomics', 'console'];
    const forbidden = function (name) {
        return function () {
            throw new Error('__D2F_FORBIDDEN__:' + name);
        };
    };
    for (const name of FORBIDDEN) {
        if (name === 'console') {
            continue;
        }
        try {
            Object.defineProperty(g, name, {get: forbidden(name), set: forbidden(name), configurable: false});
        } catch (e) {
            // 엔진이 고정한 전역은 그대로 둔다(이미 막힌 상태)
        }
    }

    const parse = JSON.parse;
    const stringify = JSON.stringify;
    const freeze = Object.freeze;
    const isFrozen = Object.isFrozen;
    const keys = Object.keys;
    const arrayFrom = Array.from;

    return function (maxLogBytes, maxLogEntries, maxArrayLength) {
        // ---- 배열 생성 길이 제한: Array.from은 큰 길이를 한 번에 할당해 취소가 늦으므로 막는다(ADR-008 간접 메모리 제한) ----
        const guardedFrom = function from(items, mapFn, thisArg) {
            if (items != null) {
                const length = items.length;
                if (typeof length === 'number' && length > maxArrayLength) {
                    throw new RangeError('배열 길이 한도(' + maxArrayLength + ')를 넘었습니다: ' + length);
                }
            }
            return arguments.length > 1 ? arrayFrom.call(this, items, mapFn, thisArg) : arrayFrom.call(this, items);
        };
        Object.defineProperty(Array, 'from', {value: guardedFrom, writable: false, configurable: false});

        // ---- console.log: 호스트 콜백 없이 안에서 모은다(1회 1KB, 최대 N건) ----
        const logs = [];
        let dropped = 0;
        const log = function () {
            if (logs.length >= maxLogEntries) {
                dropped++;
                return;
            }
            let line = '';
            for (let i = 0; i < arguments.length; i++) {
                const a = arguments[i];
                let s;
                try {
                    s = typeof a === 'string' ? a : stringify(a);
                } catch (e) {
                    s = String(a);
                }
                if (s === undefined) {
                    s = String(a);
                }
                line += (i > 0 ? ' ' : '') + s;
                if (line.length > maxLogBytes) {
                    break;
                }
            }
            if (line.length > maxLogBytes) {
                line = line.substring(0, maxLogBytes) + '…(잘림)';
            }
            logs.push(line);
        };
        const consoleObject = freeze({log: log, info: log, warn: log, error: log, debug: log});
        Object.defineProperty(g, 'console', {value: consoleObject, writable: false, configurable: false});

        // ---- 헬퍼(SCR-01.04, SCR-api §3.3). 모두 순수 함수 ----
        const round = function (x, digits) {
            const d = digits === undefined ? 0 : digits;
            if (typeof x !== 'number' || !isFinite(x)) {
                return x;
            }
            return Number(Math.round(Number(x + 'e' + d)) + 'e-' + d);
        };
        const clamp = function (x, min, max) {
            return Math.min(Math.max(x, min), max);
        };
        const c2f = function (c) {
            return c * 9 / 5 + 32;
        };
        const f2c = function (f) {
            return (f - 32) * 5 / 9;
        };
        const UNITS = {
            'C>F': c2f, 'F>C': f2c, '℃>℉': c2f, '℉>℃': f2c, 'C>K': function (v) {
                return v + 273.15;
            }, 'K>C': function (v) {
                return v - 273.15;
            },
            'hPa>kPa': function (v) {
                return v / 10;
            }, 'kPa>hPa': function (v) {
                return v * 10;
            }, 'Pa>hPa': function (v) {
                return v / 100;
            }, 'hPa>Pa': function (v) {
                return v * 100;
            },
            'mm>cm': function (v) {
                return v / 10;
            }, 'cm>mm': function (v) {
                return v * 10;
            }, 'cm>m': function (v) {
                return v / 100;
            }, 'm>cm': function (v) {
                return v * 100;
            }, 'mm>m': function (v) {
                return v / 1000;
            }, 'm>mm': function (v) {
                return v * 1000;
            }
        };
        const convert = function (value, fromUnit, toUnit) {
            if (fromUnit === toUnit) {
                return value;
            }
            const f = UNITS[fromUnit + '>' + toUnit];
            if (!f) {
                throw new RangeError('바꿀 수 없는 단위입니다: ' + fromUnit + ' → ' + toUnit);
            }
            return f(value);
        };
        // Magnus 식(a=17.62, b=243.12)
        const dewPoint = function (tempC, rh) {
            const gamma = Math.log(rh / 100) + (17.62 * tempC) / (243.12 + tempC);
            return round((243.12 * gamma) / (17.62 - gamma), 1);
        };
        // 불쾌지수(기상청 식): 0.81T + 0.01RH(0.99T − 14.3) + 46.3
        const thi = function (tempC, rh) {
            return round(0.81 * tempC + 0.01 * rh * (0.99 * tempC - 14.3) + 46.3, 1);
        };
        const absHumidity = function (tempC, rh) {
            const sat = 6.112 * Math.exp((17.67 * tempC) / (tempC + 243.5));
            return round((sat * rh * 2.1674) / (273.15 + tempC), 2);
        };

        const B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
        const fromBase64 = function (s) {
            const clean = String(s).replace(/[^A-Za-z0-9+/]/g, '');
            const out = new Uint8Array(Math.floor((clean.length * 3) / 4));
            let o = 0, buf = 0, bits = 0;
            for (let i = 0; i < clean.length; i++) {
                buf = (buf << 6) | B64.indexOf(clean.charAt(i));
                bits += 6;
                if (bits >= 8) {
                    bits -= 8;
                    out[o++] = (buf >> bits) & 0xff;
                }
            }
            return out.subarray(0, o);
        };
        const toBase64 = function (b) {
            let s = '';
            for (let i = 0; i < b.length; i += 3) {
                const n = (b[i] << 16) | ((i + 1 < b.length ? b[i + 1] : 0) << 8) | (i + 2 < b.length ? b[i + 2] : 0);
                s += B64.charAt((n >> 18) & 63) + B64.charAt((n >> 12) & 63)
                    + (i + 1 < b.length ? B64.charAt((n >> 6) & 63) : '=') + (i + 2 < b.length ? B64.charAt(n & 63) : '=');
            }
            return s;
        };
        const fromHex = function (s) {
            const clean = String(s).replace(/\s+/g, '');
            if (clean.length % 2 !== 0 || /[^0-9a-fA-F]/.test(clean)) {
                throw new RangeError('16진수 문자열이 아닙니다');
            }
            const out = new Uint8Array(clean.length / 2);
            for (let i = 0; i < out.length; i++) {
                out[i] = parseInt(clean.substr(i * 2, 2), 16);
            }
            return out;
        };
        const toHex = function (b) {
            let s = '';
            for (let i = 0; i < b.length; i++) {
                s += (b[i] < 16 ? '0' : '') + b[i].toString(16);
            }
            return s;
        };
        const need = function (b, offset, size) {
            if (!Number.isInteger(offset) || offset < 0 || offset + size > b.length) {
                throw new RangeError('offset ' + offset + '이 범위(0~' + (b.length - size) + ') 밖입니다');
            }
        };
        const u8 = function (b, o) {
            need(b, o, 1);
            return b[o] & 0xff;
        };
        const u16le = function (b, o) {
            need(b, o, 2);
            return (b[o] & 0xff) | ((b[o + 1] & 0xff) << 8);
        };
        const u16be = function (b, o) {
            need(b, o, 2);
            return ((b[o] & 0xff) << 8) | (b[o + 1] & 0xff);
        };
        const u32le = function (b, o) {
            need(b, o, 4);
            return ((b[o] & 0xff) | ((b[o + 1] & 0xff) << 8) | ((b[o + 2] & 0xff) << 16)) + (b[o + 3] & 0xff) * 0x1000000;
        };
        const u32be = function (b, o) {
            need(b, o, 4);
            return (b[o] & 0xff) * 0x1000000 + (((b[o + 1] & 0xff) << 16) | ((b[o + 2] & 0xff) << 8) | (b[o + 3] & 0xff));
        };
        const sign = function (v, bits) {
            const limit = Math.pow(2, bits - 1);
            return v >= limit ? v - limit * 2 : v;
        };
        const bytes = freeze({
            fromBase64: fromBase64, toBase64: toBase64, fromHex: fromHex, toHex: toHex,
            readUInt8: u8, readUInt16LE: u16le, readUInt16BE: u16be, readUInt32LE: u32le, readUInt32BE: u32be,
            readInt8: function (b, o) {
                return sign(u8(b, o), 8);
            },
            readInt16LE: function (b, o) {
                return sign(u16le(b, o), 16);
            },
            readInt16BE: function (b, o) {
                return sign(u16be(b, o), 16);
            },
            readInt32LE: function (b, o) {
                return sign(u32le(b, o), 32);
            },
            readInt32BE: function (b, o) {
                return sign(u32be(b, o), 32);
            }
        });

        const metric = function (msg, key) {
            const list = msg && msg.metrics ? msg.metrics : [];
            for (let i = 0; i < list.length; i++) {
                if (list[i] && list[i].key === key) {
                    return list[i];
                }
            }
            return null;
        };
        const setMetric = function (msg, key, value, unit) {
            if (!msg.metrics) {
                msg.metrics = [];
            }
            const m = metric(msg, key);
            if (m) {
                m.value = value;
                if (unit !== undefined) {
                    m.unit = unit;
                }
                return m;
            }
            const created = {key: key, value: value};
            if (unit !== undefined) {
                created.unit = unit;
            }
            msg.metrics.push(created);
            return created;
        };
        const removeMetric = function (msg, key) {
            if (msg && msg.metrics) {
                msg.metrics = msg.metrics.filter(function (m) {
                    return !m || m.key !== key;
                });
            }
            return msg;
        };

        const deepFreeze = function (o, depth) {
            if (o === null || typeof o !== 'object' || isFrozen(o) || depth > 32) {
                return o;
            }
            const ks = keys(o);
            for (let i = 0; i < ks.length; i++) {
                deepFreeze(o[ks[i]], depth + 1);
            }
            return freeze(o);
        };

        let nowIso = null;
        const util = freeze({
            round: round, clamp: clamp, c2f: c2f, f2c: f2c, cToF: c2f, fToC: f2c, convert: convert,
            dewPoint: dewPoint, thi: thi, absHumidity: absHumidity,
            movingAvg: function (key, value) {
                return value;
            },
            bytes: bytes, base64ToBytes: fromBase64, hexToBytes: fromHex, bytesToHex: toHex,
            metric: metric, setMetric: setMetric, removeMetric: removeMetric,
            now: function () {
                return nowIso;
            }
        });

        return freeze({
            /** 진입 함수 호출: 입력·컨텍스트는 JSON 문자열로 받아 안에서 푼다(호스트 객체를 넘기지 않음) */
            invoke: function (name, inputJson, ctxJson, now) {
                nowIso = now;
                const fn = g[name];
                if (typeof fn !== 'function') {
                    throw new Error('__D2F_ENTRY_MISSING__:' + name);
                }
                const input = parse(inputJson);
                const c = deepFreeze(parse(ctxJson), 0);
                const ctx = freeze({
                    config: c.config || freeze({}),
                    device: c.device || null,
                    last: c.last || freeze({}),
                    source: c.source || null,
                    modules: freeze({}),
                    util: util,
                    log: log,
                    window: function () {
                        return [];
                    }
                });
                return fn(input, ctx);
            },
            logs: function () {
                const out = logs.slice();
                if (dropped > 0) {
                    out.push('…로그 ' + dropped + '건 생략');
                }
                return out;
            }
        });
    };
})(globalThis);
