// Connects to the relay with automatic reconnect. Returns { send }.
export function connect(role, onMessage, onStatus) {
  const room = new URLSearchParams(location.search).get("room") || "default";
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const url = `${proto}://${location.host}/ws?role=${role}&room=${encodeURIComponent(room)}`;
  let ws;

  function open() {
    ws = new WebSocket(url);
    ws.onopen = () => onStatus(true);
    ws.onclose = () => {
      onStatus(false);
      setTimeout(open, 1000);
    };
    ws.onmessage = (e) => onMessage(JSON.parse(e.data));
  }
  open();

  return {
    room,
    get buffered() {
      return ws.bufferedAmount;
    },
    send(msg) {
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
    },
  };
}
