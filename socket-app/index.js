//require everything
var express = require('express');
var app = express();
var http = require('http').createServer(app);
var io = require('socket.io')(http)
//Basic NodeJS stuff 
app.use(express.static(__dirname + '/node_modules'));
app.get('/', function(req, res,next){
  res.sendFile(__dirname + '/index.html');
});

io.on('connection', function(client) {  
    console.log('Client connected...');

    client.on('join', function(data) {
        console.log(data);
    });
    });

http.listen(4200);
//Call SocketIO with the message from Kafka 
function callSockets(io, message){
    io.sockets.emit('channel', message);
}
// Init the Kafka client. Basically just make topic the same topic as your producer and you are ready to go. group-id can be anything. 
var kafka = require('kafka-node'),
    HighLevelConsumer = kafka.HighLevelConsumer,
    client = new kafka.Client(),
    consumer = new HighLevelConsumer(
        client,
        [
            { topic: 'output' }
        ],
        {
            groupId: 'sample-consumer-group'
        }
    );
  consumer.on('message', function (message) {
    //console.log(message);
    //Call our SocketIO function 
    callSockets(io,message);
     // Saving the message is optional but reccomended in case you need it again
    // In production you could write the record directly to your database. But for now we are just going to create an index of files.
  
});

