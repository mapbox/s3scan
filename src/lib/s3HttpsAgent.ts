import * as https from 'https'
import * as os from 'os'

const MAX_SOCKET = Math.ceil(os.cpus().length * 16)
const KEEP_ALIVE_MILLISECONDS = 60000

export const S3_SCAN_HTTPS_AGENT = new https.Agent({
  keepAlive: true,
  maxSockets: MAX_SOCKET,
  keepAliveMsecs: KEEP_ALIVE_MILLISECONDS,
})
