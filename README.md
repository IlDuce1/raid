- **On-Chain Indexing**
  - Subscribes to ERC20 `Transfer` events on Ethereum (via WebSocket RPC).
  - Classifies transfers into **buys** and **sells**.
  - Fetches token metadata (name, symbol, pair address) and caches results in memory + MongoDB.
  - Queries Dexscreener API for **USD price and volume** enrichment.

- **Database Persistence**
  - Stores trades in MongoDB collections (`buys`, `sells`).
  - Maintains token-level aggregates:
    - Holder list & count
    - Total volume (raw + USD)
    - Percentage increase
    - Last updated timestamp

- **Real-Time Broadcasting**
  - Separate websocket service powered by `axum` + `socketioxide`.
  - Uses MongoDB **change streams** to detect live updates to the `tokens` collection.
  - Pushes updates instantly to all connected clients via `"db_update"` events.

---

### 🗂️ Project Structure
raid/
├── raid-indexer/ # Indexer package (listens to Ethereum chain & updates MongoDB)
│ ├── src/
│ └── Cargo.toml
├── raid-ws/ # Websocket broadcaster package
│ ├── src/
│ └── Cargo.toml
├── Cargo.toml # Workspace manifest
└── README.md

### 1. Clone & Install
```bash
git clone https://github.com/IlDuce1/raid.git
cd raid

# Build all packages
cargo build

# Run the indexer
cargo run -p raid-indexer

# Run the websocket server
cargo run -p raid-ws

# ENV
MONGODB_CONNECTION_STRING=mongodb://localhost:27017
RPC_URL=YOUR/RPC/URL

### Example
import { io } from "socket.io-client";

const socket = io("http://localhost:3000");

socket.on("connect", () => {
  console.log("Connected:", socket.id);
});

socket.on("db_update", (data) => {
  console.log("Token update:", JSON.parse(data));
});
