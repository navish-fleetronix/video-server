'use strict';

const net  = require('net');
const http = require('http');
const fs   = require('fs');
const path = require('path');
require('dotenv').config();
const { WebSocketServer } = require('ws');
const { spawn } = require('child_process');

// ── Config ────────────────────────────────────────────────────────────────────
const CONFIG = {
    tcpPort:  3007,
    httpPort: 8080,
    wsPort:   8801,
    serverIp: process.env.SERVER_IP,
};
console.log(`Server IP: ${CONFIG.serverIp}`);

if (!fs.existsSync('./public')) fs.mkdirSync('./public');

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: CONFIG.wsPort });
console.log(`✓ WebSocket on :${CONFIG.wsPort}`);

function wsBroadcast(obj) {
    const msg = JSON.stringify(obj);
    wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}

// ── HTTP server ───────────────────────────────────────────────────────────────
http.createServer((req, res) => {
    const filePath = req.url === '/' ? './video.html' : `.${req.url}`;
    const ext = path.extname(filePath).toLowerCase();
    const contentTypes = {
        '.html': 'text/html', '.js': 'application/javascript',
        '.m3u8': 'application/vnd.apple.mpegurl', '.ts': 'video/mp2t',
    };
    fs.readFile(filePath, (err, data) => {
        if (err) { res.writeHead(404); res.end('Not found'); return; }
        res.writeHead(200, {
            'Content-Type': contentTypes[ext] || 'text/plain',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-cache',
        });
        res.end(data);
    });
}).listen(CONFIG.httpPort, () => console.log(`✓ HTTP on :${CONFIG.httpPort}`));

// ── FFmpeg for HLS (AVS video + AAC audio) ───────────────────────────────────
// Camera sends AVS video (codec 100, T/98 Table 12) + G.711A or AAC audio.
// We wrap both in MPEG-TS and let FFmpeg transcode to H.264+AAC for HLS.
function startFFmpeg(channel) {
    const ffmpeg = spawn('/usr/local/bin/ffmpeg', [
        '-fflags',          '+genpts+discardcorrupt+igndts',
        '-err_detect',      'ignore_err',
        '-f',               'mpegts',
        '-probesize',       '500000',
        '-analyzeduration', '1000000',
        '-i',               'pipe:0',
        // Video: transcode AVS → H.264
        '-c:v',             'libx264',
        '-preset',          'ultrafast',
        '-tune',            'zerolatency',
        '-g',               '50',
        '-keyint_min',      '25',
        // Audio: transcode whatever codec → AAC for browser HLS
        '-c:a',             'aac',
        '-ar',              '8000',   // G.711A is 8 kHz; AAC inherits sample rate
        '-ac',              '1',      // mono
        '-b:a',             '32k',
        // HLS output
        '-f',               'hls',
        '-hls_time',        '1',
        '-hls_list_size',   '3',
        '-hls_flags',       'delete_segments+append_list',
        '-hls_segment_filename', `./public/ch${channel}_%03d.ts`,
        `./public/ch${channel}.m3u8`,
    ]);

    ffmpeg.stderr.on('data', d => {
        const msg = d.toString().trim();
        if (msg.includes('Error') || msg.includes('Invalid') || msg.includes('error')) {
            console.error(`FFmpeg ch${channel}:`, msg);
        }
    });

    ffmpeg.on('close', code => {
        console.log(`FFmpeg ch${channel} closed (code ${code}), restarting in 1s...`);
        setTimeout(() => { channels[channel].ffmpeg = startFFmpeg(channel); }, 1000);
    });

    return ffmpeg;
}

const channels = {
    1: { ffmpeg: null, gotIFrame: false, subpackets: [] },
};
channels[1].ffmpeg = startFFmpeg(1);

// ── MPEG-TS muxer (AVS video + G.711A/AAC audio) ─────────────────────────────
// PIDs:  0=PAT  4096=PMT  256=video  257=audio
// Stream types: 0x42=AVS video  0x90=G.711A audio (private)
const TS_PACKET_SIZE = 188;
const VIDEO_PID      = 256;
const AUDIO_PID      = 257;
const PMT_PID        = 4096;

// Continuity counters per PID (must increment per packet on the same PID)
const cc = { [VIDEO_PID]: 0, [AUDIO_PID]: 0, [PMT_PID]: 0, 0: 0 };

function buildPAT() {
    const pkt = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
    pkt[0] = 0x47; pkt[1] = 0x40; pkt[2] = 0x00;
    pkt[3] = 0x10 | (cc[0]++ & 0x0F);
    pkt[4] = 0x00; // pointer field
    const s = pkt.slice(5);
    s[0]  = 0x00;              // table_id = PAT
    s[1]  = 0xB0; s[2] = 0x0D;
    s[3]  = 0x00; s[4] = 0x01; // ts_id=1
    s[5]  = 0xC1; s[6] = 0x00; s[7] = 0x00;
    s[8]  = 0x00; s[9] = 0x01; // program 1
    s[10] = (PMT_PID >> 8) | 0xE0;
    s[11] = PMT_PID & 0xFF;
    return pkt;
}

function buildPMT() {
    // Two stream entries: video (0x42=AVS) + audio (0x90=G.711A private)
    const pkt = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
    pkt[0] = 0x47;
    pkt[1] = 0x40 | ((PMT_PID >> 8) & 0x1F);
    pkt[2] = PMT_PID & 0xFF;
    pkt[3] = 0x10 | (cc[PMT_PID]++ & 0x0F);
    pkt[4] = 0x00;
    const s = pkt.slice(5);
    s[0]  = 0x02;
    s[1]  = 0xB0; s[2] = 0x1C; // section length = 28
    s[3]  = 0x00; s[4] = 0x01;
    s[5]  = 0xC1; s[6] = 0x00; s[7] = 0x00;
    s[8]  = 0xE0 | ((VIDEO_PID >> 8) & 0x1F);
    s[9]  = VIDEO_PID & 0xFF;  // PCR PID
    s[10] = 0xF0; s[11] = 0x00;
    // Video stream: type 0x42 = AVS
    s[12] = 0x42;
    s[13] = 0xE0 | ((VIDEO_PID >> 8) & 0x1F);
    s[14] = VIDEO_PID & 0xFF;
    s[15] = 0xF0; s[16] = 0x00;
    // Audio stream: type 0x90 = private (G.711A / PCM_ALAW)
    // FFmpeg will detect from the PES stream_id 0xC0
    s[17] = 0x90;
    s[18] = 0xE0 | ((AUDIO_PID >> 8) & 0x1F);
    s[19] = AUDIO_PID & 0xFF;
    s[20] = 0xF0; s[21] = 0x00;
    return pkt;
}

function wrapInTS(pid, pesData, isStart) {
    const packets = [];
    let pos = 0; let first = isStart;

    while (pos < pesData.length) {
        const pkt  = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
        pkt[0] = 0x47;
        pkt[1] = (first ? 0x40 : 0x00) | ((pid >> 8) & 0x1F);
        pkt[2] = pid & 0xFF;
        pkt[3] = 0x10 | (cc[pid] & 0x0F);
        cc[pid] = (cc[pid] + 1) & 0x0F;

        const room  = TS_PACKET_SIZE - 4;
        const chunk = pesData.slice(pos, pos + room);
        chunk.copy(pkt, 4);
        pos  += chunk.length;
        first = false;
        packets.push(pkt);
    }
    return packets;
}

function makePES(streamId, payload) {
    const hdr = Buffer.from([
        0x00, 0x00, 0x01,  // start code
        streamId,          // 0xE0 = video, 0xC0 = audio
        0x00, 0x00,        // packet_length = 0 (unbounded for video); for audio FFmpeg is fine
        0x80, 0x00, 0x00,  // flags, no PTS
    ]);
    return Buffer.concat([hdr, payload]);
}

let patPmtSent = false;

function sendToFFmpeg(channel, pid, rawPayload, streamId) {
    const ch = channels[channel];
    if (!ch || !ch.ffmpeg || !ch.ffmpeg.stdin.writable) return;

    if (!patPmtSent) {
        ch.ffmpeg.stdin.write(buildPAT());
        ch.ffmpeg.stdin.write(buildPMT());
        patPmtSent = true;
        console.log(`ch${channel} 📺 Sent PAT+PMT (AVS video 0x42 + G.711A audio 0x90)`);
    }

    const pes     = makePES(streamId, rawPayload);
    const packets = wrapInTS(pid, pes, true);
    for (const pkt of packets) ch.ffmpeg.stdin.write(pkt);
}

// ── Handle one complete stream frame ─────────────────────────────────────────
// dataType per T/98 §5.5.3 Table 19:
//   0x00 = Video I-frame  0x01 = Video P-frame  0x02 = Video B-frame
//   0x03 = Audio frame    0x04 = Transparent data
function handleVideoFrame(frameData, channel, dataType) {
    const ch = channels[channel];
    if (!ch) return;

    if (dataType === 3) {
        // Audio frame — G.711A (stream_id 0xC0 = audio)
        sendToFFmpeg(channel, AUDIO_PID, frameData, 0xC0);
        return;
    }

    const isVideo = (dataType === 0 || dataType === 1 || dataType === 2);
    if (!isVideo) return;

    if (dataType === 0) {
        ch.gotIFrame = true;
        console.log(`ch${channel} ✅ I_FRAME size:${frameData.length}`);
    } else {
        if (!ch.gotIFrame) return;
        console.log(`ch${channel} ${dataType === 1 ? 'P' : 'B'}_FRAME size:${frameData.length}`);
    }

    // Video — AVS (stream_id 0xE0 = video)
    sendToFFmpeg(channel, VIDEO_PID, frameData, 0xE0);
}

// ── Reassemble subpackets ─────────────────────────────────────────────────────
function processVideoPacket(rawData, channel, dataType, subpktMarker) {
    const ch = channels[channel];
    if (!ch) return;

    if (subpktMarker === 0) {
        handleVideoFrame(rawData, channel, dataType);
    } else if (subpktMarker === 1) {
        ch.subpackets = [rawData];
    } else if (subpktMarker === 3) {
        if (ch.subpackets.length > 0) ch.subpackets.push(rawData);
    } else if (subpktMarker === 2) {
        if (ch.subpackets.length > 0) {
            ch.subpackets.push(rawData);
            const complete = Buffer.concat(ch.subpackets);
            ch.subpackets  = [];
            console.log(`ch${channel} complete frame size:${complete.length}`);
            handleVideoFrame(complete, channel, dataType);
        }
    }
}

// ── JT/T 808 protocol helpers ─────────────────────────────────────────────────
function unescapeBuffer(buf) {
    const out = []; let i = 0;
    while (i < buf.length) {
        if      (buf[i] === 0x7D && buf[i+1] === 0x02) { out.push(0x7E); i += 2; }
        else if (buf[i] === 0x7D && buf[i+1] === 0x01) { out.push(0x7D); i += 2; }
        else { out.push(buf[i++]); }
    }
    return Buffer.from(out);
}

function escapeBuffer(buf) {
    const out = [];
    for (const b of buf) {
        if      (b === 0x7E) { out.push(0x7D, 0x02); }
        else if (b === 0x7D) { out.push(0x7D, 0x01); }
        else { out.push(b); }
    }
    return Buffer.from(out);
}

function buildFrame(msgId, body, phone) {
    const header = Buffer.alloc(12);
    header.writeUInt16BE(msgId,       0);
    header.writeUInt16BE(body.length, 2);
    Buffer.from(phone.match(/.{2}/g).map(h => parseInt(h, 16))).copy(header, 4);
    header.writeUInt16BE(Math.floor(Math.random() * 0xFFFF), 10);
    const payload = Buffer.concat([header, body]);
    let cs = 0; payload.forEach(b => cs ^= b);
    return Buffer.concat([
        Buffer.from([0x7E]),
        escapeBuffer(Buffer.concat([payload, Buffer.from([cs])])),
        Buffer.from([0x7E]),
    ]);
}

function buildAck(phone, replySeq, replyMsgId) {
    const body = Buffer.alloc(5);
    body.writeUInt16BE(replySeq,   0);
    body.writeUInt16BE(replyMsgId, 2);
    body[4] = 0; // success
    return buildFrame(0x8001, body, phone);
}

function buildRegisterResponse(phone, replySeq, result, authCode) {
    const authBuf = Buffer.from(authCode, 'ascii');
    const body    = Buffer.alloc(3 + authBuf.length);
    body.writeUInt16BE(replySeq, 0);
    body[2] = result;
    authBuf.copy(body, 3);
    return buildFrame(0x8100, body, phone);
}

// T/98 §5.5.1 Table 17 — real-time audio+video request
function buildVideoRequest(phone, serverIp, serverPort, channel) {
    const ipBuf = Buffer.from(serverIp, 'ascii');
    const N     = ipBuf.length;
    const body  = Buffer.alloc(8 + N);
    body[0] = N;
    ipBuf.copy(body, 1);
    body.writeUInt16BE(serverPort, 1 + N); // TCP port
    body.writeUInt16BE(0,          3 + N); // UDP port = 0
    body[5 + N] = channel;
    body[6 + N] = 0; // dataType 0 = Audio+Video (was 1=VideoOnly before)
    body[7 + N] = 0; // main stream
    return buildFrame(0x9101, body, phone);
}

// T/98 §5.6.1 Table 21 — query resource list (all channels, no time/alarm filter)
function buildQueryRecordings(phone) {
    const body = Buffer.alloc(23);
    body[0]  = 0;      // channel 0 = all channels
    // bytes 1-6:  start time BCD all-zero = no filter
    // bytes 7-12: end time BCD all-zero = no filter
    // bytes 13-20: alarm logo all-zero = no filter
    body[21] = 2;      // resource type: 2=Video only
    body[22] = 0;      // stream type: 0=all
    return buildFrame(0x9205, body, phone);
}

// Parse BCD date from terminal response (6 bytes: YY MM DD HH MM SS)
function parseBCD6(buf, offset) {
    const b = i => ((buf[offset+i] >> 4) * 10 + (buf[offset+i] & 0x0F));
    return `20${String(b(0)).padStart(2,'0')}-${String(b(1)).padStart(2,'0')}-${String(b(2)).padStart(2,'0')} ${String(b(3)).padStart(2,'0')}:${String(b(4)).padStart(2,'0')}:${String(b(5)).padStart(2,'0')}`;
}

// T/98 §5.6.2 Table 22+23 — parse terminal's recording list response
function parseRecordingList(body) {
    console.log(`[Recordings] raw body hex (first 80 bytes): ${body.slice(0, 80).toString('hex')}`);
    console.log(`[Recordings] body length: ${body.length}`);

    if (body.length < 6) {
        console.log(`[Recordings] ⚠️  Body too short`);
        return [];
    }

    const seq   = body.readUInt16BE(0);
    const total = body.readUInt32BE(2);
    console.log(`[Recordings] seq=${seq} total_reported=${total}`);

    if (total === 0) {
        console.log(`[Recordings] ⚠️  Device reports 0 recordings`);
        return [];
    }

    const recordings = [];
    let i = 6;
    let entry = 0;

    while (i < body.length) {
        const remaining = body.length - i;
        if (remaining < 28) {
            console.log(`[Recordings] ⚠️  Only ${remaining} bytes left at entry ${entry}, need 28 — stopping`);
            break;
        }
        const ch         = body[i];
        const startTime  = parseBCD6(body, i + 1);
        const endTime    = parseBCD6(body, i + 7);
        const alarmHex   = body.slice(i + 13, i + 21).toString('hex');
        const avType     = body[i + 21];
        const streamType = body[i + 22];
        const memType    = body[i + 23];
        const fileSize   = body.readUInt32BE(i + 24);
        console.log(`[Recordings] entry[${entry}]: ch=${ch} start=${startTime} end=${endTime} avType=${avType} size=${fileSize} alarm=${alarmHex}`);
        recordings.push({ ch, startTime, endTime, avType, streamType, memType, size: fileSize });
        i += 28;
        entry++;
    }

    console.log(`[Recordings] parsed ${recordings.length} of ${total} reported`);
    return recordings;
}

function parseAdditionalInfo(buf) {
    const result = {};
    let i = 0;
    while (i < buf.length - 2) {
        const id  = buf[i];
        const len = buf[i+1];
        if (i + 2 + len > buf.length) break;
        const val = buf.slice(i+2, i+2+len);
        switch(id) {
            case 0x01: if (val.length >= 4) result.mileage        = val.readUInt32BE(0) / 10 + ' km';   break;
            case 0x03: if (val.length >= 2) result.sensorSpeed    = val.readUInt16BE(0) / 10 + ' km/h'; break;
            case 0x25: if (val.length >= 2) result.voltage        = val.readUInt16BE(0) / 10 + ' V';    break;
            case 0x30: if (val.length >= 1) result.signalStrength  = val[0];                            break;
            case 0x31: if (val.length >= 1) result.satellites     = val[0];                             break;
        }
        i += 2 + len;
    }
    return result;
}

// ── TCP server ────────────────────────────────────────────────────────────────
const tcpServer = net.createServer(socket => {
    const remote = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`Device connected: ${remote}`);
    let buffer = Buffer.alloc(0);
    let phone  = null;

    socket.on('data', data => {
        try {
            buffer = Buffer.concat([buffer, data]);
            let offset = 0;

            while (offset < buffer.length - 4) {

                // ── Stream data packet (T/98 §5.5.3 — magic 0x30316364) ──────
                if (buffer[offset]   === 0x30 && buffer[offset+1] === 0x31 &&
                    buffer[offset+2] === 0x63 && buffer[offset+3] === 0x64) {

                    if (offset + 30 > buffer.length) break;
                    const dataBodyLen = buffer.readUInt16BE(offset + 28);
                    if (offset + 30 + dataBodyLen > buffer.length) break;

                    const byte15       = buffer[offset + 15];
                    const dataType     = (byte15 >> 4) & 0x0F;
                    const subpktMarker = byte15 & 0x0F;
                    const channel      = buffer[offset + 14];
                    const rawData      = buffer.slice(offset + 30, offset + 30 + dataBodyLen);

                    processVideoPacket(rawData, channel, dataType, subpktMarker);
                    offset += 30 + dataBodyLen;
                    continue;
                }

                // ── Signalling packet (JT/T 808 — 0x7E framing) ─────────────
                if (buffer[offset] === 0x7E) {
                    const end = buffer.indexOf(0x7E, offset + 1);
                    if (end === -1) break;

                    const inner     = buffer.slice(offset + 1, end);
                    const unescaped = unescapeBuffer(inner);
                    if (unescaped.length < 12) { offset = end + 1; continue; }

                    const msgId = unescaped.readUInt16BE(0);
                    phone = unescaped.slice(4, 10)
                        .map(b => `${(b >> 4) & 0x0F}${b & 0x0F}`)
                        .join('').replace(/^0+/, '');
                    const seq  = unescaped.readUInt16BE(10);
                    const body = unescaped.slice(12);
                    console.log(`[signalling] msgId: 0x${msgId.toString(16).padStart(4,'0')} phone: ${phone}`);

                    if (msgId === 0x0100) {
                        // Registration
                        socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));

                    } else if (msgId === 0x0102) {
                        // Auth success → start live stream + query recordings
                        socket.write(buildAck(phone, seq, msgId));
                        socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 1));
                        // Delay the recordings query slightly so the device is ready
                        setTimeout(() => {
                            socket.write(buildQueryRecordings(phone));
                        }, 2000);

                    } else if (msgId === 0x1205) {
                        // Terminal's recording list response (T/98 §5.6.2)
                        socket.write(buildAck(phone, seq, msgId));
                        const recordings = parseRecordingList(body);
                        wsBroadcast({ type: 'recordings', data: recordings });

                    } else if (msgId === 0x0200) {
                        // Location report
                        socket.write(buildAck(phone, seq, msgId));

                        const alarmFlags = body.readUInt32BE(0);
                        const statusBits = body.readUInt32BE(4);
                        const latRaw     = body.readUInt32BE(8);
                        const lonRaw     = body.readUInt32BE(12);
                        const elevation  = body.readUInt16BE(16);
                        const speed      = body.readUInt16BE(18) / 10;
                        const direction  = body.readUInt16BE(20);

                        const south   = !!(statusBits & (1 << 2));
                        const west    = !!(statusBits & (1 << 3));
                        const lat     = latRaw / 1e6 * (south ? -1 : 1);
                        const lon     = lonRaw / 1e6 * (west  ? -1 : 1);
                        const accOn   = !!(statusBits & (1 << 0));
                        const located = !!(statusBits & (1 << 1));

                        const bcd = b => ((b >> 4) * 10 + (b & 0x0F));
                        const t   = 22;
                        const dt  = `20${String(bcd(body[t])).padStart(2,'0')}-${String(bcd(body[t+1])).padStart(2,'0')}-${String(bcd(body[t+2])).padStart(2,'0')} ${String(bcd(body[t+3])).padStart(2,'0')}:${String(bcd(body[t+4])).padStart(2,'0')}:${String(bcd(body[t+5])).padStart(2,'0')}`;

                        const alarms = [];
                        if (alarmFlags & (1<<0)) alarms.push('Emergency');
                        if (alarmFlags & (1<<1)) alarms.push('Overspeed');
                        if (alarmFlags & (1<<4)) alarms.push('GNSS Fault');
                        if (alarmFlags & (1<<5)) alarms.push('GNSS Antenna Cut');
                        if (alarmFlags & (1<<7)) alarms.push('Low Voltage');
                        if (alarmFlags & (1<<8)) alarms.push('Power Off');

                        const locationData = {
                            type: 'location', phone, lat, lon, speed, direction,
                            elevation, datetime: dt, accOn, located, alarms,
                            ...parseAdditionalInfo(body.slice(27))
                        };

                        console.log(`[GPS] ${phone} lat=${lat} lon=${lon} speed=${speed}km/h dir=${direction}° dt=${dt}`);
                        wsBroadcast(locationData);

                        // GPS log
                        const fileName  = `gps_log_${new Date().toISOString().slice(0,10)}.txt`;
                        const gpsRecord = {
                            phone, datetime: dt, latitude: lat, longitude: lon,
                            speed_kmh: speed, direction_deg: direction, elevation_m: elevation,
                            acc:             accOn   ? 'ON'    : 'OFF',
                            located:         located ? 'YES'   : 'NO',
                            mileage:         locationData.mileage        || '--',
                            voltage:         locationData.voltage        || '--',
                            satellites:      locationData.satellites     || '--',
                            signal:          locationData.signalStrength || '--',
                            sensor_speed:    locationData.sensorSpeed    || '--',
                            oil_circuit:     !!(statusBits & (1<<10)) ? 'CUT'  : 'NORMAL',
                            vehicle_circuit: !!(statusBits & (1<<11)) ? 'CUT'  : 'NORMAL',
                            door:            !!(statusBits & (1<<13)) ? 'OPEN' : 'CLOSED',
                            alarms: alarmFlags !== 0 ? [
                                (alarmFlags & (1<<0)) ? 'EMERGENCY'   : null,
                                (alarmFlags & (1<<1)) ? 'OVERSPEED'   : null,
                                (alarmFlags & (1<<4)) ? 'GNSS_FAULT'  : null,
                                (alarmFlags & (1<<5)) ? 'ANTENNA_CUT' : null,
                                (alarmFlags & (1<<7)) ? 'LOW_VOLTAGE' : null,
                                (alarmFlags & (1<<8)) ? 'POWER_OFF'   : null,
                            ].filter(Boolean).join('|') : 'NONE',
                        };
                        fs.appendFile(`./${fileName}`, Object.values(gpsRecord).join(',') + '\n', err => {
                            if (err) console.error('[GPS LOG] write error:', err.message);
                        });

                    } else {
                        // Log any unexpected message IDs — helps diagnose recordings issues
                        console.log(`[signalling] ⚠️  Unhandled msgId=0x${msgId.toString(16).padStart(4,'0')} bodyLen=${body.length} hex=${body.slice(0,32).toString('hex')}`);
                        socket.write(buildAck(phone, seq, msgId));
                    }

                    offset = end + 1;
                    continue;
                }

                offset++;
            }

            buffer = buffer.slice(offset);

        } catch (err) {
            console.error('Error processing data:', err.message);
        }
    });

    socket.on('close', () => console.log(`Device disconnected: ${remote}`));
    socket.on('error', err => console.error(`Socket error: ${err.message}`));
});

tcpServer.listen(CONFIG.tcpPort, () => {
    console.log(`✓ TCP server on :${CONFIG.tcpPort}`);
});