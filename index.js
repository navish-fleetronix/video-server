'use strict';

const net  = require('net');
const http = require('http');
const fs   = require('fs');
const path = require('path');
require('dotenv').config();
const { WebSocketServer } = require('ws');
const { spawn } = require('child_process');
const { env } = require('process');

// ── Config ────────────────────────────────────────────────────────────────────
const CONFIG = {
    tcpPort:  3007,
    httpPort: 8080,
    wsPort:   8801,
    serverIp: process.env.SERVER_IP
};
console.log(`Server IP: ${CONFIG.serverIp}`);
// ── Create output folder ──────────────────────────────────────────────────────
if (!fs.existsSync('./public')) fs.mkdirSync('./public');

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: CONFIG.wsPort });
console.log(`✓ WebSocket on :${CONFIG.wsPort}`);

// ── HTTP server (serves video.html) ──────────────────────────────────────────
http.createServer((req, res) => {
    let filePath = req.url === '/' ? './video.html' : `.${req.url}`;
    if (req.url.startsWith('/public/')) filePath = `.${req.url}`;

    const ext = path.extname(filePath).toLowerCase();
    const contentTypes = {
        '.html': 'text/html',
        '.js':   'application/javascript',
        '.css':  'text/css',
        '.wasm': 'application/wasm',
        '.m3u8': 'application/vnd.apple.mpegurl',
        '.ts':   'video/mp2t',
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
}).listen(CONFIG.httpPort, () => {
    console.log(`✓ HTTP on :${CONFIG.httpPort}`);
});

// ── FFmpeg for HLS ────────────────────────────────────────────────────────────
function startFFmpeg(channel) {
    const ffmpeg = spawn('ffmpeg', [
        '-f',            'h264',
        '-i',            'pipe:0',
        '-c:v',          'copy',
        '-f',            'hls',
        '-hls_time',     '1',
        '-hls_list_size','3',
        '-hls_flags',    'delete_segments+append_list',
        `./public/ch${channel}.m3u8`,
    ]);
    ffmpeg.stderr.on('data', d => console.log(`FFmpeg ch${channel}:`, d.toString().trim()));
    ffmpeg.on('close', code => {
        console.log(`FFmpeg ch${channel} closed, restarting...`);
        setTimeout(() => { channels[channel].ffmpeg = startFFmpeg(channel); }, 1000);
    });
    return ffmpeg;
}

const channels = {
    1: { ffmpeg: null, gotIFrame: false, subpackets: [] },
    2: { ffmpeg: null, gotIFrame: false, subpackets: [] },
};

channels[1].ffmpeg = startFFmpeg(1);
channels[2].ffmpeg = startFFmpeg(2);

// ── Handle complete H.264 frame ───────────────────────────────────────────────
function handleVideoFrame(h264Data, channel, dataType) {
    const ch = channels[channel];
    if (!ch) return;

    if (dataType === 0) {
        ch.gotIFrame = true;
        console.log(`ch${channel} I_FRAME size:${h264Data.length}`);
    }

    if (!ch.gotIFrame) return;

    // Write to FFmpeg
    if (ch.ffmpeg && ch.ffmpeg.stdin.writable) {
        ch.ffmpeg.stdin.write(h264Data);
    }

    // Send via WebSocket
    const header  = Buffer.from([channel, dataType]);
    const message = Buffer.concat([header, h264Data]);
    wss.clients.forEach(client => {
        if (client.readyState === 1) client.send(message, { binary: true });
    });
}

// ── Reassemble subpackets ─────────────────────────────────────────────────────
function processVideoPacket(h264Data, channel, dataType, subpktMarker) {
    const ch = channels[channel];
    if (!ch) return;

    console.log(`ch${channel} subpkt:${subpktMarker} dataType:${dataType} bytes:${h264Data.length}`);

    if (subpktMarker === 0) {
        // Atomic — complete frame
        handleVideoFrame(h264Data, channel, dataType);

    } else if (subpktMarker === 1) {
        // First piece
        ch.subpackets = [h264Data];

    } else if (subpktMarker === 3) {
        // Middle piece
        ch.subpackets.push(h264Data);

    } else if (subpktMarker === 2) {
        // Last piece — join all
        ch.subpackets.push(h264Data);
        const complete = Buffer.concat(ch.subpackets);
        ch.subpackets = [];
        console.log(`ch${channel} complete frame NAL: ${complete[0].toString(16)} ${complete[1].toString(16)} ${complete[2].toString(16)} ${complete[3].toString(16)}`);
        handleVideoFrame(complete, channel, dataType);
    }
}

// ── Helper functions ──────────────────────────────────────────────────────────
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
    body[4] = 0;
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

function buildVideoRequest(phone, serverIp, serverPort, channel) {
    const ipBuf = Buffer.from(serverIp, 'ascii');
    const N     = ipBuf.length;
    const body  = Buffer.alloc(8 + N);
    body[0] = N;
    ipBuf.copy(body, 1);
    body.writeUInt16BE(serverPort, 1 + N);
    body.writeUInt16BE(0,          3 + N);
    body[5 + N] = channel;
    body[6 + N] = 1; // video only
    body[7 + N] = 0; // main stream
    return buildFrame(0x9101, body, phone);
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

                // ── Video frame ───────────────────────────────────────────────
                if (buffer[offset]   === 0x30 && buffer[offset+1] === 0x31 &&
                    buffer[offset+2] === 0x63 && buffer[offset+3] === 0x64) {

                    if (offset + 30 > buffer.length) break;

                    const dataBodyLen = buffer.readUInt16BE(offset + 28);
                    if (offset + 30 + dataBodyLen > buffer.length) break;

                    const byte15       = buffer[offset + 15];
                    const dataType     = (byte15 >> 4) & 0x0F;
                    const subpktMarker = byte15 & 0x0F;
                    const channel      = buffer[offset + 14];
                    const h264Data     = buffer.slice(offset + 30, offset + 30 + dataBodyLen);

                    processVideoPacket(h264Data, channel, dataType, subpktMarker);

                    offset += 30 + dataBodyLen;
                    continue;
                }

                // ── Signalling frame ──────────────────────────────────────────
                if (buffer[offset] === 0x7E) {
                    const end = buffer.indexOf(0x7E, offset + 1);
                    if (end === -1) break;

                    const inner     = buffer.slice(offset + 1, end);
                    const unescaped = unescapeBuffer(inner);
                    if (unescaped.length < 12) { offset = end + 1; continue; }

                    const msgId = unescaped.readUInt16BE(0);
                    phone       = unescaped.slice(4, 10).map(b => b.toString(16).padStart(2,'0')).join('');
                    const seq   = unescaped.readUInt16BE(10);

                    console.log(`[signalling] msgId: 0x${msgId.toString(16).padStart(4,'0')} phone: ${phone}`);

                    if (msgId === 0x0100) {
                        // Registration
                        socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));

                    } else if (msgId === 0x0102) {
                        // Auth → request video
                        socket.write(buildAck(phone, seq, msgId));
                        socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 1));
                        socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 2));

                    } else {
                        socket.write(buildAck(phone, seq, msgId));
                    }

                    offset = end + 1;
                    continue;
                }

                offset++;
            }

            // Keep unprocessed data
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