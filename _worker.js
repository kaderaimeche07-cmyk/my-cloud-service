import { connect } from 'cloudflare:sockets';

const userID = '8f4e2a19-6c3b-4d8e-9a12-7b5f0c3e8d41';

export default {
  async fetch(request) {
    const upgrade = request.headers.get('Upgrade');
    const url = new URL(request.url);

    if (!upgrade || upgrade.toLowerCase() !== 'websocket') {
      if (url.pathname === '/' + userID) {
        const host = url.hostname;
        const link = `vless://${userID}@${host}:443?encryption=none&security=tls&sni=${host}&type=ws&host=${host}&path=%2F%3Fed%3D2048#MyServer`;
        return new Response(link, {
          status: 200,
          headers: { 'Content-Type': 'text/plain;charset=utf-8' }
        });
      }
      return new Response('System Active', { status: 200 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    handleWS(server);
    return new Response(null, { status: 101, webSocket: client });
  }
};

async function handleWS(ws) {
  let remote = null;
  let first = true;

  ws.addEventListener('message', async (e) => {
    try {
      const data = new Uint8Array(e.data);
      if (first) {
        first = false;
        const res = parseVless(data, userID);
        if (res.err) { ws.close(); return; }
        remote = connect({ hostname: res.host, port: res.port });
        const writer = remote.writable.getWriter();
        await writer.write(data.slice(res.idx));
        writer.releaseLock();
        readRemote(remote, ws);
      } else if (remote) {
        const writer = remote.writable.getWriter();
        await writer.write(data);
        writer.releaseLock();
      }
    } catch { ws.close(); }
  });

  ws.addEventListener('close', () => { if (remote) remote.close(); });
}

function readRemote(remote, ws) {
  const reader = remote.readable.getReader();
  (async () => {
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (ws.readyState === 1) ws.send(value);
      }
    } catch {}
    ws.close();
  })();
}

function parseVless(buf, id) {
  if (buf.byteLength < 24) return { err: true };
  const hex = [...buf.slice(1, 17)].map(b => b.toString(16).padStart(2, '0')).join('');
  const uuid = `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
  if (uuid !== id) return { err: true };

  const optLen = buf[17];
  if (buf[18 + optLen] !== 1) return { err: true };

  const pIdx = 19 + optLen;
  const port = (buf[pIdx] << 8) | buf[pIdx + 1];
  const type = buf[pIdx + 2];
  let idx = pIdx + 3;
  let host = '';

  if (type === 1) {
    host = buf.slice(idx, idx + 4).join('.');
    idx += 4;
  } else if (type === 2) {
    const len = buf[idx++];
    host = new TextDecoder().decode(buf.slice(idx, idx + len));
    idx += len;
  } else if (type === 3) {
    const p = [];
    for (let i = 0; i < 8; i++) p.push(((buf[idx + i * 2] << 8) | buf[idx + i * 2 + 1]).toString(16));
    host = p.join(':');
    idx += 16;
  } else return { err: true };

  return { err: false, host, port, idx };
}
