const net = require("net");
const fs = require("fs");
const path = require("path");

console.log("Logs from your program will appear here!");

const args = process.argv.slice(2);
let dir = "./";
let dbfilename = "dump.rdb";

//map
let map = new Map();
const commandSet = new Set(["ping", "echo", "set", "get", "config", "keys"]);

for (let i = 0; i < args.length; i++) {
  if (args[i] === "--dir") dir = args[i + 1];
  else if (args[i] === "--dbfilename") dbfilename = args[i + 1];
}
const pathToFile = path.join(dir, dbfilename);
if (fs.existsSync(pathToFile)) {
  let rdbBuffer = fs.readFileSync(pathToFile);
  let metadata = "";
  let currIdx = 9;
  const MAX_DELAY = 2_147_483_647;
  let key;
  let value;
  //skip over metadata and db index to reach key value pairs
  if (rdbBuffer[currIdx] === 0xfa) {
    for (let i = 10; i < rdbBuffer.length; i++) {
      if (rdbBuffer[i] === 0xfe) {
        currIdx = i + 5;
        break;
      }
      metadata += String.fromCharCode(rdbBuffer[i]);
    }
  }
  while (rdbBuffer[currIdx] != 0xff) {
    //check for timestamp
    let timestamp = null;
    let delay = null;
    if (rdbBuffer[currIdx] === 0xfc) {
      currIdx++;
      timestamp = Number(rdbBuffer.readBigUInt64LE(currIdx));
      currIdx += 8;
      delay = timestamp - Date.now();
    } else if (rdbBuffer[currIdx] === 0xfd) {
      currIdx++;
      timestamp = Number(rdbBuffer.readBigUInt32LE(currIdx)) * 1000; //convert s to ms
      currIdx += 4;
      delay = timestamp - Date.now();
    }

    let typeFlag = rdbBuffer[currIdx++];
    if (typeFlag === 0x00) {
      //string
      let dataLen = rdbBuffer[currIdx++];
      //key
      key = rdbBuffer.slice(currIdx, dataLen + currIdx).toString();
      currIdx += dataLen;
      //value
      dataLen = rdbBuffer[currIdx++];
      value = rdbBuffer.slice(currIdx, dataLen + currIdx).toString();
      currIdx += dataLen;
      //map
      if (timestamp === null || delay >= 0) map.set(key, value);
      if (timestamp !== null && delay < MAX_DELAY && delay >= 0)
        timer(timestamp, key);
    }
  }
} else {
}

//redis parser
class Parser {
  constructor() {
    this.buffer = "";
    this.queue = [];
  }

  buildBuffer(data) {
    this.buffer += data.toString();
    this._parse();
    return this.queue;
  }

  _parse() {
    while (true) {
      const result = this._getType();
      if (result === null) break;
      this.queue.push(result);
    }
  }

  _getType() {
    if (this.buffer.length === 0) return null;
    const type = this.buffer[0];
    switch (type) {
      case "+":
        return this._parseSimpleString();
      case "-":
        return this._parseError();
      case ":":
        return this._parseInteger();
      case "$":
        return this._parseBulkString();
      case "*":
        return this._parseArray();
      default:
        throw new Error(`Unknown type: ${type}`);
    }
  }

  _parseSimpleString() {
    // +this is a simple string\r\n
    let commandArr = [];
    let endIdx = this.buffer.indexOf("\r\n");
    if (endIdx == -1) return null;
    let parsedString = this.buffer.slice(1, endIdx);
    this.buffer = this.buffer.slice(endIdx + 2);
    commandArr.push(parsedString);
    return commandArr;
  }

  _parseError() {
    // -ERR unknown command "FLUHS"\r\n
    let commandArr = [];
    let endIdx = this.buffer.indexOf("\r\n");
    if (endIdx == -1) return null;
    let parsedError = this.buffer.slice(1, endIdx);
    this.buffer = this.buffer.slice(endIdx + 2);
    commandArr.push(new Error(parsedError));
    return commandArr;
  }

  _parseInteger() {
    // :1337\r\n
    let commandArr = [];
    let endIdx = this.buffer.indexOf("\r\n");
    if (endIdx == -1) return null;
    let parsedInteger = Number(this.buffer.slice(1, endIdx));
    this.buffer = this.buffer.slice(endIdx + 2);
    commandArr.push(parsedInteger);
    return commandArr;
  }

  _parseBulkString() {
    // $5\r\nhello\r\n
    let endIdx = this.buffer.indexOf("\r\n");
    if (endIdx == -1) return null;
    let strLen = Number(this.buffer.slice(1, endIdx));
    if (strLen == -1) return null;

    this.buffer = this.buffer.slice(endIdx + 2);
    let parsedBulkString = this.buffer.slice(0, strLen);
    this.buffer = this.buffer.slice(strLen + 2);
    return parsedBulkString;
  }

  _parseArray() {
    // *3\r\n+a simple string element\r\n
    // :12345\r\n
    // $7\r\ntesting\r\n
    let commandArr = [];
    let endIdx = this.buffer.indexOf("\r\n");
    if (endIdx == -1) return null;
    let arrLen = Number(this.buffer.slice(1, endIdx));
    if (arrLen == -1) return null;
    this.buffer = this.buffer.slice(endIdx + 2);

    while (arrLen > 0) {
      let value = this._getType();
      if (Array.isArray(value)) {
        commandArr.push(...value);
      } else {
        commandArr.push(value);
      }
      arrLen--;
    }
    // console.log(commandArr);
    // this.queue.push(commandArr);
    return commandArr;
  }

  _encodeResponse(data) {
    if (typeof data === "string") {
      return Buffer.from(`$${data.length}\r\n${data}\r\n`);
    } else if (typeof data === "number") {
      return Buffer.from(`:${data}\r\n`);
    } else if (data instanceof Error) {
      return Buffer.from(`-${data.message}\r\n`);
    } else if (data == null) {
      return Buffer.from(`$-1\r\n`);
    } else if (Array.isArray(data)) {
      const items = data.map((item) => this._encodeResponse(item));
      return Buffer.concat([Buffer.from(`*${data.length}\r\n`), ...items]);
    } else {
      return new Error("unknown data type");
    }
  }
}

//server
const server = net.createServer((connection) => {
  // Handle connection
  const redisParser = new Parser();
  // console.log("map at the start: ", map);

  connection.on("data", (data) => {
    let commandQueue = redisParser.buildBuffer(data);
    console.log(commandQueue);

    for (const commandArr of commandQueue) {
      while (commandQueue.length > 0) {
        const commandArr = commandQueue.shift();
        let command = commandArr[0].toLowerCase();
        // console.log("command ", command);
        if (command === "ping") {
          connection.write(redisParser._encodeResponse("PONG"));
        } else if (command === "echo") {
          commandArr.shift();
          const encoded = commandArr.map((item) =>
            redisParser._encodeResponse(item)
          );
          encoded.forEach((buf) => connection.write(buf));
        } else if (command === "set") {
          commandArr.shift();
          const key = commandArr.shift();
          const value = commandArr.shift();
          // console.log(key, value);
          try {
            map.set(key, value);
            connection.write(Buffer.from("+OK\r\n"));
          } catch (err) {
            throw new Error("error setting value:", { cause: err });
          }

          if (commandArr.length > 0) {
            let args = commandArr.shift().toLowerCase();
            if (args === "px") {
              if (commandArr.length > 0) {
                let expiry = Number(commandArr.shift());
                timer(expiry, key);
              } else {
                throw new Error("error setting px, timer missing");
              }
            }
          }
          // console.log("map at the end of set: ", map);
          // console.log("queue in set: ", commandQueue);
        } else if (command === "get") {
          commandArr.shift();
          const key = commandArr.shift();
          if (map.has(key)) {
            connection.write(redisParser._encodeResponse(map.get(key)));
          } else {
            connection.write(redisParser._encodeResponse(null));
          }
          // console.log("queue in get: ", commandQueue);
        } else if (command === "config") {
          commandArr.shift();
          if (commandArr.length > 0) {
            let args = commandArr.shift().toLowerCase();
            if (args === "get") {
              if (commandArr.length > 0) {
                let param = commandArr.shift().toLowerCase();
                if (param === "dir") {
                  connection.write(redisParser._encodeResponse(["dir", dir]));
                } else if (param === "dbfilename") {
                  connection.write(
                    redisParser._encodeResponse(["dbfilename", dbfilename])
                  );
                }
              }
            }
          }
        } else if (command === "keys") {
          commandArr.shift();
          let args = commandArr.shift().toLowerCase();
          if (args === "*") {
            const keysArray = Array.from(map.keys());
            connection.write(redisParser._encodeResponse(keysArray));
          }
        }
        // console.log("map at the end of iteration: ", map);
        // console.log("queue at the end of iteration: ", commandQueue);
      }
    }
  });
});

server.listen(6379, "127.0.0.1");

function timer(expiry, key) {
  setTimeout(() => {
    deleteItem(key);
  }, expiry);
}

function deleteItem(key) {
  if (map.has(key)) map.delete(key);
}

// function save() {
//   let pathToFile = path.join(dir, dbfilename);
//   try {
//     if (!fs.existsSync(dir)) {
//       fs.mkdirSync(dir, { recursive: true });
//     }
//     createRDBFile(pathToFile);
//   } catch (err) {
//     throw new Error(err);
//   }
// }

//"$18\r\nSET foo bar px 100\r\n"
