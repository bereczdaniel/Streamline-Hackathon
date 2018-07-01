'use strict';

const {Client, ConsumerGroup, HighLevelProducer} = require('kafka-node');
const bodyParser = require('body-parser')
const express = require('express')
const app = express()
const net = require('net')
const path = require('path')

var http = require('http').Server(app)
var io = require('socket.io')(http)

//state
let actor_actor_val = {}
function set_relation(c1, c2, relation){
	if(!(c1 in actor_actor_val)){
		actor_actor_val[c1] = {}
	}
	actor_actor_val[c1][c2] = relation
}

//express
app.use(bodyParser.json())
app.use(bodyParser.urlencoded({
  extended: true
}))
app.use(express.static('public'))

app.get('/', (req, res) => {
	res.header("Access-Control-Allow-Origin", "*")
	res.sendFile(path.join(__dirname, 'public', 'index.html'))
})
app.get('/state', (req, res) => {
	res.header("Access-Control-Allow-Origin", "*")
	res.send(actor_actor_val)
})
http.listen(8081, () => {
	console.log('GDELT server listening on 8081!')
})

//kafka consume updates
var topics = ['incrementalUpdate', 'fullStateUpdate', 'test']
let groupId = "group-" + Math.floor(Math.random() * 10000)
let consumerGroup = new ConsumerGroup({
  host: '127.0.0.1:2181',
  id: "consumer1",
  groupId: groupId,
  sessionTimeout: 15000,
  autoCommit:false,
  protocol: ['roundrobin'],
  fromOffset: 'latest',
  outOfRangeOffset: 'latest',
  commitOffsetsOnFirstJoin:false,
}, topics);

consumerGroup.on('message', function (message) {
	if(message.topic == 'fullStateUpdate'){
		console.log('fullStateUpdate')
		let msg = JSON.parse(message.value)
		for(let tup of msg){
			set_relation(tup._1, tup._2, tup._3)
		}
		io.emit('state', msg.map(function(d){return {
			actor1: d._1,
			actor2: d._2,
			score: d._3
		}}))
	} else if(message.topic == 'incrementalUpdate'){
		let {actor1, actor2, score} = JSON.parse(message.value)
		set_relation(actor1, actor2, score)
		io.emit('score', {actor1: actor1, actor2:actor2, score:score})
	}
});

//send request for initial sync
let kakfaClientId = "worker-" + Math.floor(Math.random() * 10000)
var client = new Client('localhost:2181', kakfaClientId);
var producer = new HighLevelProducer(client);
producer.on('ready', function () {
    producer.send([{ topic: 'stateRequest', messages: 'yes' }], function (err, data) {});
});


//kill kafka consumer on interrupt
process.on('SIGINT', function () {
	consumer.close(true, function () {
		process.exit();
	});
});
