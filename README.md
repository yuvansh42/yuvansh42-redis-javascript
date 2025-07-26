[![progress-banner](https://backend.codecrafters.io/progress/redis/3c642b6d-8eaf-4dbf-9121-d4d8323cb4d8)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

# 🔴 Redis Clone in JavaScript (Codecrafters Inspired)

This is a minimal Redis clone written in Node.js as part of the Codecrafters Redis challenge.  
It supports a subset of the Redis protocol and implements core commands and TTL functionality.

---

## 🚀 Features

- ✅ RESP protocol parsing (`+`, `-`, `:`, `$`, `*`)
- ✅ Core commands: `PING`, `ECHO`, `SET`, `GET`, `CONFIG GET`, `KEYS *`
- ✅ Supports `PX` for expiry in milliseconds
- ✅ In-memory key-value store with TTL
- ✅ Partial RDB file parsing and key restoration
- ⚠️ No persistence/saving yet (`SAVE` not implemented)

---

## 🛠 How to Run

```bash
npm install
node app/main.js --dir ./data --dbfilename dump.rdb
