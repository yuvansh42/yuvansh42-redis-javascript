const net = require("net");
// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
  // Handle connection
  // Handle connection
  connection.on("data", (data) => {
    connection.write("+PONG\r\n");
    console.log(data.toString().split("\n"));
  });
  connection.on("end", () => {
    console.log("client disconnected");
  });
});
server.listen(6379, "127.0.0.1");