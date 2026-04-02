// 'use strict';

// const net  = require('net');
// const http = require('http');
// const fs   = require('fs');
// const path = require('path');
// require('dotenv').config();
// const { WebSocketServer } = require('ws');
// const { spawn } = require('child_process');
// const { env } = require('process');

// // ── Config ────────────────────────────────────────────────────────────────────
// const CONFIG = {
//     tcpPort:  3007,
//     httpPort: 8080,
//     wsPort:   8801,
//     serverIp: process.env.SERVER_IP
// };
// console.log(`Server IP: ${CONFIG.serverIp}`);
// // ── Create output folder ──────────────────────────────────────────────────────
// if (!fs.existsSync('./public')) fs.mkdirSync('./public');

// // ── WebSocket server ──────────────────────────────────────────────────────────
// const wss = new WebSocketServer({ port: CONFIG.wsPort });
// console.log(`✓ WebSocket on :${CONFIG.wsPort}`);

// // ── HTTP server (serves video.html) ──────────────────────────────────────────
// http.createServer((req, res) => {
//     let filePath;
    
//     if (req.url === '/') {
//         filePath = './video.html';
//     } else if (req.url.startsWith('/public/')) {
//         filePath = `.${req.url}`;  // → ./public/ch1.m3u8
//     } else {
//         filePath = `.${req.url}`;
//     }

//     console.log('Serving:', filePath); // ← add this to debug

//     const ext = path.extname(filePath).toLowerCase();
//     const contentTypes = {
//         '.html': 'text/html',
//         '.js':   'application/javascript',
//         '.m3u8': 'application/vnd.apple.mpegurl',
//         '.ts':   'video/mp2t',
//     };

//     fs.readFile(filePath, (err, data) => {
//         if (err) { 
//             console.error('File not found:', filePath);
//             res.writeHead(404); 
//             res.end('Not found'); 
//             return; 
//         }
//         res.writeHead(200, {
//             'Content-Type': contentTypes[ext] || 'text/plain',
//             'Access-Control-Allow-Origin': '*',
//             'Cache-Control': 'no-cache',
//         });
//         res.end(data);
//     });
// }).listen(CONFIG.httpPort, () => {
//     console.log(`✓ HTTP on :${CONFIG.httpPort}`);
// });

// // ── FFmpeg for HLS ────────────────────────────────────────────────────────────
// function startFFmpeg(channel) {
//     const ffmpeg = spawn('/usr/local/bin/ffmpeg', [
//     '-fflags',        '+genpts+discardcorrupt+igndts',
//     '-err_detect',    'ignore_err',
//     '-f',             'h264',
//     '-i',             'pipe:0',
//     '-c:v',           'libx264',
//     '-preset',        'ultrafast',
//     '-tune',          'zerolatency',
//     '-f',             'hls',
//     '-hls_time',      '1',
//     '-hls_list_size', '3',
//     '-hls_flags',     'delete_segments+append_list',
//     `./public/ch${channel}.m3u8`,
// ]);
//     ffmpeg.stderr.on('data', d => console.log(`FFmpeg ch${channel}:`, d.toString().trim()));
//     ffmpeg.on('close', code => {
//         console.log(`FFmpeg ch${channel} closed, restarting...`);
//         setTimeout(() => { channels[channel].ffmpeg = startFFmpeg(channel); }, 1000);
//     });
//     return ffmpeg;
// }

// const channels = {
//     1: { ffmpeg: null, gotIFrame: false, subpackets: [] },
//     2: { ffmpeg: null, gotIFrame: false, subpackets: [] },
// };

// channels[1].ffmpeg = startFFmpeg(1);
// channels[2].ffmpeg = startFFmpeg(2);

// // ── Handle complete H.264 frame ───────────────────────────────────────────────
// function handleVideoFrame(h264Data, channel, dataType) {
//     const ch = channels[channel];
//     if (!ch) return;

//     if (dataType === 0) {
//         ch.gotIFrame = true;
//         console.log(`ch${channel} ✅ I_FRAME size:${h264Data.length}`);
//     }

//     if (!ch.gotIFrame) return;

//     // Collect NAL units
//     const nalUnits = [];
//     let i = 0;
//     while (i < h264Data.length - 4) {
//         if (h264Data[i] === 0 && h264Data[i+1] === 0 && 
//             h264Data[i+2] === 0 && h264Data[i+3] === 1) {
            
//             // Find next NAL start
//             let next = h264Data.length;
//             for (let j = i + 4; j < h264Data.length - 4; j++) {
//                 if (h264Data[j] === 0 && h264Data[j+1] === 0 && 
//                     h264Data[j+2] === 0 && h264Data[j+3] === 1) {
//                     next = j;
//                     break;
//                 }
//             }

//             const nalType = h264Data[i+4] & 0x1F;
//             const nalData = h264Data.slice(i, next);
//             nalUnits.push({ type: nalType, data: nalData });
//             console.log(`ch${channel} NAL type:${nalType} size:${nalData.length}`);
//             i = next;
//         } else {
//             i++;
//         }
//     }

//     // Check if data partitioning (types 2,3,4)
//     const hasDP = nalUnits.some(n => n.type === 2);
//     if (hasDP) {
//         const converted = convertDataPartitioning(h264Data);
//         if (ch.ffmpeg && ch.ffmpeg.stdin.writable) {
//         ch.ffmpeg.stdin.write(converted);
//         }
//         return;
//         // const part2 = nalUnits.find(n => n.type === 2);
//         // const part3 = nalUnits.find(n => n.type === 3);
//         // const part4 = nalUnits.find(n => n.type === 4);

//         // // Combine all parts into one frame
//         // const parts = [part2, part3, part4].filter(Boolean).map(n => n.data);
//         // const combined = Buffer.concat(parts);
//         // console.log(`ch${channel} DP combined size:${combined.length}`);

//         // if (ch.ffmpeg && ch.ffmpeg.stdin.writable) {
//         //     ch.ffmpeg.stdin.write(combined);
//         // }
//         return;
//     }

//     // Normal frame
//     if (ch.ffmpeg && ch.ffmpeg.stdin.writable) {
//         ch.ffmpeg.stdin.write(h264Data);
//     }
// }
// function convertDataPartitioning(h264Data) {
//     const output = Buffer.from(h264Data); // copy
//     let i = 0;
//     while (i < output.length - 5) {
//         if (output[i] === 0 && output[i+1] === 0 && 
//             output[i+2] === 0 && output[i+3] === 1) {
            
//             const nalType = output[i+4] & 0x1F;
//             const nalRef  = (output[i+4] >> 5) & 0x03;

//             if (nalType === 2) {
//                 // Convert partition A → normal slice (type 1)
//                 output[i+4] = (nalRef << 5) | 1;
//                 console.log('Converted NAL 2 → 1');
//             } else if (nalType === 3 || nalType === 4) {
//                 // Remove partition B and C by zeroing them out
//                 output[i+4] = 0;
//             }
//             i += 4;
//         } else {
//             i++;
//         }
//     }
//     return output;
// }
// // ── Reassemble subpackets ─────────────────────────────────────────────────────
// function processVideoPacket(h264Data, channel, dataType, subpktMarker) {
//     const ch = channels[channel];
//     if (!ch) return;

//     console.log(`ch${channel} subpkt:${subpktMarker} dataType:${dataType} bytes:${h264Data.length}`);

//     if (subpktMarker === 0) {
//         // Atomic — complete frame
//         handleVideoFrame(h264Data, channel, dataType);

//     } else if (subpktMarker === 1) {
//         // First piece
//         ch.subpackets = [h264Data];

//     } else if (subpktMarker === 3) {
//         // Middle piece
//         ch.subpackets.push(h264Data);

//     } else if (subpktMarker === 2) {
//         // Last piece — join all
//         ch.subpackets.push(h264Data);
//         const complete = Buffer.concat(ch.subpackets);
//         ch.subpackets = [];
//         console.log(`ch${channel} complete frame NAL: ${complete[0].toString(16)} ${complete[1].toString(16)} ${complete[2].toString(16)} ${complete[3].toString(16)}`);
//         handleVideoFrame(complete, channel, dataType);
//     }
// }

// // ── Helper functions ──────────────────────────────────────────────────────────
// function unescapeBuffer(buf) {
//     const out = []; let i = 0;
//     while (i < buf.length) {
//         if      (buf[i] === 0x7D && buf[i+1] === 0x02) { out.push(0x7E); i += 2; }
//         else if (buf[i] === 0x7D && buf[i+1] === 0x01) { out.push(0x7D); i += 2; }
//         else { out.push(buf[i++]); }
//     }
//     return Buffer.from(out);
// }

// function escapeBuffer(buf) {
//     const out = [];
//     for (const b of buf) {
//         if      (b === 0x7E) { out.push(0x7D, 0x02); }
//         else if (b === 0x7D) { out.push(0x7D, 0x01); }
//         else { out.push(b); }
//     }
//     return Buffer.from(out);
// }

// function buildFrame(msgId, body, phone) {
//     const header = Buffer.alloc(12);
//     header.writeUInt16BE(msgId,       0);
//     header.writeUInt16BE(body.length, 2);
//     Buffer.from(phone.match(/.{2}/g).map(h => parseInt(h, 16))).copy(header, 4);
//     header.writeUInt16BE(Math.floor(Math.random() * 0xFFFF), 10);
//     const payload = Buffer.concat([header, body]);
//     let cs = 0; payload.forEach(b => cs ^= b);
//     return Buffer.concat([
//         Buffer.from([0x7E]),
//         escapeBuffer(Buffer.concat([payload, Buffer.from([cs])])),
//         Buffer.from([0x7E]),
//     ]);
// }

// function buildAck(phone, replySeq, replyMsgId) {
//     const body = Buffer.alloc(5);
//     body.writeUInt16BE(replySeq,   0);
//     body.writeUInt16BE(replyMsgId, 2);
//     body[4] = 0;
//     return buildFrame(0x8001, body, phone);
// }

// function buildRegisterResponse(phone, replySeq, result, authCode) {
//     const authBuf = Buffer.from(authCode, 'ascii');
//     const body    = Buffer.alloc(3 + authBuf.length);
//     body.writeUInt16BE(replySeq, 0);
//     body[2] = result;
//     authBuf.copy(body, 3);
//     return buildFrame(0x8100, body, phone);
// }

// function buildVideoRequest(phone, serverIp, serverPort, channel) {
//     const ipBuf = Buffer.from(serverIp, 'ascii');
//     const N     = ipBuf.length;
//     const body  = Buffer.alloc(8 + N);
//     body[0] = N;
//     ipBuf.copy(body, 1);
//     body.writeUInt16BE(serverPort, 1 + N);
//     body.writeUInt16BE(0,          3 + N);
//     body[5 + N] = channel;
//     body[6 + N] = 1; // video only
//     body[7 + N] = 0; // main stream
//     return buildFrame(0x9101, body, phone);
// }

// // ── TCP server ────────────────────────────────────────────────────────────────
// const tcpServer = net.createServer(socket => {
//     const remote = `${socket.remoteAddress}:${socket.remotePort}`;
//     console.log(`Device connected: ${remote}`);

//     let buffer = Buffer.alloc(0);
//     let phone  = null;

//     socket.on('data', data => {
//         try {
//             buffer = Buffer.concat([buffer, data]);
//             let offset = 0;

//             while (offset < buffer.length - 4) {

//                 // ── Video frame ───────────────────────────────────────────────
//                 if (buffer[offset]   === 0x30 && buffer[offset+1] === 0x31 &&
//                     buffer[offset+2] === 0x63 && buffer[offset+3] === 0x64) {

//                     if (offset + 30 > buffer.length) break;

//                     const dataBodyLen = buffer.readUInt16BE(offset + 28);
//                     if (offset + 30 + dataBodyLen > buffer.length) break;

//                     const byte15       = buffer[offset + 15];
//                     const dataType     = (byte15 >> 4) & 0x0F;
//                     const subpktMarker = byte15 & 0x0F;
//                     const channel      = buffer[offset + 14];
//                     const h264Data     = buffer.slice(offset + 30, offset + 30 + dataBodyLen);

//                     processVideoPacket(h264Data, channel, dataType, subpktMarker);

//                     offset += 30 + dataBodyLen;
//                     continue;
//                 }

//                 // ── Signalling frame ──────────────────────────────────────────
//                 if (buffer[offset] === 0x7E) {
//                     const end = buffer.indexOf(0x7E, offset + 1);
//                     if (end === -1) break;

//                     const inner     = buffer.slice(offset + 1, end);
//                     const unescaped = unescapeBuffer(inner);
//                     if (unescaped.length < 12) { offset = end + 1; continue; }

//                     const msgId = unescaped.readUInt16BE(0);
//                     phone       = unescaped.slice(4, 10).map(b => b.toString(16).padStart(2,'0')).join('');
//                     const seq   = unescaped.readUInt16BE(10);

//                     console.log(`[signalling] msgId: 0x${msgId.toString(16).padStart(4,'0')} phone: ${phone}`);

//                     if (msgId === 0x0100) {
//                         // Registration
//                         socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));

//                     } else if (msgId === 0x0102) {
//                         // Auth → request video
//                         socket.write(buildAck(phone, seq, msgId));
//                         socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 1));
//                         socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 2));

//                     } else {
//                         socket.write(buildAck(phone, seq, msgId));
//                     }

//                     offset = end + 1;
//                     continue;
//                 }

//                 offset++;
//             }

//             // Keep unprocessed data
//             buffer = buffer.slice(offset);

//         } catch (err) {
//             console.error('Error processing data:', err.message);
//         }
//     });

//     socket.on('close', () => console.log(`Device disconnected: ${remote}`));
//     socket.on('error', err => console.error(`Socket error: ${err.message}`));
// });

// tcpServer.listen(CONFIG.tcpPort, () => {
//     console.log(`✓ TCP server on :${CONFIG.tcpPort}`);
// });

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
    serverIp: process.env.SERVER_IP
};
console.log(`Server IP: ${CONFIG.serverIp}`);

// ── Create output folder ──────────────────────────────────────────────────────
if (!fs.existsSync('./public')) fs.mkdirSync('./public');

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: CONFIG.wsPort });
console.log(`✓ WebSocket on :${CONFIG.wsPort}`);

// ── HTTP server ───────────────────────────────────────────────────────────────
http.createServer((req, res) => {
    let filePath;
    if (req.url === '/')                  filePath = './video.html';
    else if (req.url.startsWith('/public/')) filePath = `.${req.url}`;
    else                                  filePath = `.${req.url}`;

    const ext = path.extname(filePath).toLowerCase();
    const contentTypes = {
        '.html': 'text/html',
        '.js':   'application/javascript',
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
    const ffmpeg = spawn('/usr/local/bin/ffmpeg', [
        '-fflags',        '+genpts+discardcorrupt+igndts',
        '-err_detect',    'ignore_err',
        '-f',             'h264',
        '-i',             'pipe:0',
        '-c:v',           'libx264',
        '-preset',        'ultrafast',
        '-tune',          'zerolatency',
        '-f',             'hls',
        '-hls_time',      '1',
        '-hls_list_size', '3',
        '-hls_flags',     'delete_segments+append_list',
        `./public/ch${channel}.m3u8`,
    ]);
    ffmpeg.stderr.on('data', d => console.log(`FFmpeg ch${channel}:`, d.toString().trim()));
    ffmpeg.on('close', () => {
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
        console.log(`ch${channel} ✅ I_FRAME size:${h264Data.length}`);
    }

    if (!ch.gotIFrame) return;

    const nalUnits = [];
    let i = 0;
    while (i < h264Data.length - 4) {
        if (h264Data[i] === 0 && h264Data[i+1] === 0 &&
            h264Data[i+2] === 0 && h264Data[i+3] === 1) {
            let next = h264Data.length;
            for (let j = i + 4; j < h264Data.length - 4; j++) {
                if (h264Data[j] === 0 && h264Data[j+1] === 0 &&
                    h264Data[j+2] === 0 && h264Data[j+3] === 1) {
                    next = j; break;
                }
            }
            const nalType = h264Data[i+4] & 0x1F;
            nalUnits.push({ type: nalType, data: h264Data.slice(i, next) });
            i = next;
        } else { i++; }
    }

    const hasDP = nalUnits.some(n => n.type === 2);
    if (hasDP) {
        const converted = convertDataPartitioning(h264Data);
        if (ch.ffmpeg && ch.ffmpeg.stdin.writable) ch.ffmpeg.stdin.write(converted);
        return;
    }

    if (ch.ffmpeg && ch.ffmpeg.stdin.writable) ch.ffmpeg.stdin.write(h264Data);
}

function convertDataPartitioning(h264Data) {
    const output = Buffer.from(h264Data);
    let i = 0;
    while (i < output.length - 5) {
        if (output[i] === 0 && output[i+1] === 0 &&
            output[i+2] === 0 && output[i+3] === 1) {
            const nalType = output[i+4] & 0x1F;
            const nalRef  = (output[i+4] >> 5) & 0x03;
            if (nalType === 2) output[i+4] = (nalRef << 5) | 1;
            else if (nalType === 3 || nalType === 4) output[i+4] = 0;
            i += 4;
        } else { i++; }
    }
    return output;
}

// ── Reassemble subpackets ─────────────────────────────────────────────────────
function processVideoPacket(h264Data, channel, dataType, subpktMarker) {
    const ch = channels[channel];
    if (!ch) return;

    if (subpktMarker === 0) {
        handleVideoFrame(h264Data, channel, dataType);
    } else if (subpktMarker === 1) {
        ch.subpackets = [h264Data];
    } else if (subpktMarker === 3) {
        ch.subpackets.push(h264Data);
    } else if (subpktMarker === 2) {
        ch.subpackets.push(h264Data);
        const complete = Buffer.concat(ch.subpackets);
        ch.subpackets = [];
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
    body[6 + N] = 1;
    body[7 + N] = 0;
    return buildFrame(0x9101, body, phone);
}

// ── NEW: BCD time encode/decode ───────────────────────────────────────────────
function encodeBcdTime(date, buf, offset) {
    const d = new Date(date);
    buf[offset]   = parseInt(d.getFullYear().toString().slice(2), 10);
    buf[offset+1] = d.getMonth() + 1;
    buf[offset+2] = d.getDate();
    buf[offset+3] = d.getHours();
    buf[offset+4] = d.getMinutes();
    buf[offset+5] = d.getSeconds();
    // Convert each to BCD
    for (let i = offset; i < offset + 6; i++) {
        buf[i] = ((Math.floor(buf[i] / 10) << 4) | (buf[i] % 10));
    }
}

function readBcdTime(buf, offset) {
    const bcd = b => ((b >> 4) * 10 + (b & 0x0F));
    const yy = bcd(buf[offset]);
    const mo = bcd(buf[offset+1]);
    const dd = bcd(buf[offset+2]);
    const hh = bcd(buf[offset+3]);
    const mm = bcd(buf[offset+4]);
    const ss = bcd(buf[offset+5]);
    return `20${String(yy).padStart(2,'0')}-${String(mo).padStart(2,'0')}-${String(dd).padStart(2,'0')} ${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}:${String(ss).padStart(2,'0')}`;
}

// ── NEW: Query resource list (0x9205) ─────────────────────────────────────────
function buildQueryResourceList(phone, channel, startTime, endTime) {
    const body = Buffer.alloc(23);
    body[0] = channel;                    // logical channel (0=all)
    encodeBcdTime(startTime, body, 1);    // start time BCD[6]
    encodeBcdTime(endTime,   body, 7);    // end time BCD[6]
    body.fill(0, 13, 21);                 // alarm flag 64 bits = 0 (no filter)
    body[21] = 2;                         // resource type: 2=video
    body[22] = 1;                         // stream type: 1=main stream
    return buildFrame(0x9205, body, phone);
}

// ── NEW: Playback request (0x9201) ────────────────────────────────────────────
function buildPlaybackRequest(phone, channel, startTime, endTime) {
    const ipBuf = Buffer.from(CONFIG.serverIp, 'ascii');
    const N     = ipBuf.length;
    const body  = Buffer.alloc(23 + N);
    body[0] = N;
    ipBuf.copy(body, 1);
    body.writeUInt16BE(CONFIG.tcpPort, 1 + N);
    body.writeUInt16BE(0,              3 + N);
    body[5 + N] = channel;
    body[6 + N] = 2;   // video only
    body[7 + N] = 1;   // main stream
    body[8 + N] = 0;   // main memory
    body[9 + N] = 0;   // normal playback
    body[10+ N] = 0;   // reserved
    encodeBcdTime(startTime, body, 11 + N);
    encodeBcdTime(endTime,   body, 17 + N);
    return buildFrame(0x9201, body, phone);
}

// ── NEW: Handle resource list response ────────────────────────────────────────
function handleResourceList(body, socket, phone) {
    if (body.length < 6) return;
    const total = body.readUInt32BE(2);
    console.log(`Total recordings: ${total}`);

    const recordings = [];
    let offset = 6;
    while (offset + 28 <= body.length) {
        const ch        = body[offset];
        const startTime = readBcdTime(body, offset + 1);
        const endTime   = readBcdTime(body, offset + 7);
        const size      = body.readUInt32BE(offset + 24);
        recordings.push({ ch, startTime, endTime, size });
        console.log(`Recording: ch=${ch} start=${startTime} end=${endTime} size=${size} bytes`);
        offset += 28;
    }

    // Send list to all WebSocket clients (browser can show a list)
    wss.clients.forEach(client => {
        if (client.readyState === 1) {
            client.send(JSON.stringify({ type: 'recordings', data: recordings }));
        }
    });

    // Auto play first recording if available
    if (recordings.length > 0) {
        const rec = recordings[0];
        console.log(`Starting playback: ch=${rec.ch} ${rec.startTime} → ${rec.endTime}`);
        socket.write(buildPlaybackRequest(phone, rec.ch, rec.startTime, rec.endTime));
    }
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
                    const body  = unescaped.slice(12);

                    console.log(`[signalling] msgId: 0x${msgId.toString(16).padStart(4,'0')} phone: ${phone}`);

                    if (msgId === 0x0100) {
                        // Registration
                        socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));

                    } else if (msgId === 0x0102) {
                        // Auth → request live video
                        socket.write(buildAck(phone, seq, msgId));
                        socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 1));
                        socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 2));

                        // ← NEW: Query recordings from last 1 hour
                        const now   = new Date();
                        const start = new Date(now - 60 * 60 * 1000);
                        socket.write(buildQueryResourceList(phone, 0, start, now)); // 0 = all channels

                    } else if (msgId === 0x1205) {
                        // ← NEW: Resource list response
                        socket.write(buildAck(phone, seq, msgId));
                        handleResourceList(body, socket, phone);

                    } else {
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

// In WebSocket server section add:
wss.on('connection', ws => {
    ws.on('message', msg => {
        try {
            const data = JSON.parse(msg);
            if (data.type === 'playback_request') {
                console.log('Browser requested playback:', data);
                // Find the socket for this device and send playback request
                activeSockets.forEach(({ socket, phone }) => {
                    socket.write(buildPlaybackRequest(
                        phone,
                        data.channel,
                        data.startTime,
                        data.endTime
                    ));
                });
            }
        } catch (_) {}
    });
});
// ```

// **What was added:**
// 1. `encodeBcdTime` / `readBcdTime` — encode/decode BCD timestamps
// 2. `buildQueryResourceList` — query recordings from device (`0x9205`)
// 3. `buildPlaybackRequest` — start playback of a recording (`0x9201`)
// 4. `handleResourceList` — parse recording list and send to browser
// 5. After auth — automatically queries last 1 hour of recordings
// 6. Handles `0x1205` response — logs recordings and starts playback

// Check logs for:
// ```
// Total recordings: X
// Recording: ch=1 start=2025-XX-XX end=2025-XX-XX size=XXXX bytes