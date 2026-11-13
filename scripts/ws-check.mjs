// Connects to the dashboard WebSocket (through nginx) like the browser does and checks that
// metrics, health and transaction messages arrive. Uses the dashboard's own STOMP client library.
import { Client } from '../dashboard/node_modules/@stomp/stompjs/esm6/index.js';

const url = (process.env.DASH ?? 'http://localhost:3000').replace(/^http/, 'ws') + '/ws';
const seen = { metrics: 0, health: 0, transactions: 0 };
const client = new Client({ brokerURL: url, reconnectDelay: 0 });
const timer = setTimeout(() => finish(false), 15000);

client.onConnect = () => {
  for (const topic of Object.keys(seen)) {
    client.subscribe(`/topic/${topic}`, (m) => {
      JSON.parse(m.body);
      seen[topic]++;
      if (Object.values(seen).every((n) => n > 0)) finish(true);
    });
  }
};
client.onStompError = () => finish(false);

function finish(ok) {
  clearTimeout(timer);
  console.log(`  ws messages: ${JSON.stringify(seen)}`);
  client.deactivate().finally(() => process.exit(ok ? 0 : 1));
}
client.activate();
