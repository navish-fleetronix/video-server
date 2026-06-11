'use strict';

// Load both services in the same process
// so device-bus EventEmitter is shared in memory
require('./ftp-service');
require('./index-pictor');