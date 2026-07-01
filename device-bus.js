'use strict';

/**
 * device-bus.js
 *
 * Shared event bus between index-pictor.js / index-acumen.js and
 * ftp-service.js. Receivers don't import each other or ftp-service — they
 * only talk through this bus.
 *
 * Every event carries a `vendor` field ('pictor' | 'acumen') identifying
 * which receiver the device is connected to. This matters because phone
 * numbers are only unique *within* a vendor's fleet — two receivers could
 * see the same phone number for two different physical devices.
 *
 * Events emitted by index-pictor.js / index-acumen.js:
 *   'device:connected'    { vendor, phone, socket }
 *   'device:disconnected' { vendor, phone }
 *   'device:message'      { vendor, msgId, body, seq, phone, socket }
 *
 * Events emitted by ftp-service.js:
 *   'device:send'         { vendor, phone, frame }   ← the receiver for that
 *                                                        vendor writes it to
 *                                                        the device's socket
 */

const EventEmitter = require('events');
const bus = new EventEmitter();
bus.setMaxListeners(20);

module.exports = bus;