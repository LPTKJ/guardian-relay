package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	devices   = make(map[string]*Device)
	devicesMu sync.RWMutex
)

type Device struct {
	ID        string
	Conn      *websocket.Conn
	Parents   map[*websocket.Conn]bool
	ParentsMu sync.RWMutex
	LastSeen  time.Time
}

type Message struct {
	Type     string `json:"type"`
	DeviceID string `json:"device_id,omitempty"`
	Data     string `json:"data,omitempty"`
}

func main() {
	port := flag.String("port", "8080", "listen port")
	flag.Parse()

	http.HandleFunc("/ws", handleWebSocket)
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/api/devices", handleDevices)

	log.Printf("Guardian Relay Server started on :%s", *port)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	deviceID := r.URL.Query().Get("device")
	if deviceID == "" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, deviceListHTML)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, parentHTML, deviceID)
}

func handleDevices(w http.ResponseWriter, r *http.Request) {
	devicesMu.RLock()
	defer devicesMu.RUnlock()

	type DeviceInfo struct {
		ID       string `json:"id"`
		Parents  int    `json:"parents"`
		LastSeen int64  `json:"last_seen"`
	}

	list := make([]DeviceInfo, 0, len(devices))
	for id, dev := range devices {
		list = append(list, DeviceInfo{
			ID:       id,
			Parents:  len(dev.Parents),
			LastSeen: dev.LastSeen.Unix(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"devices": list,
		"count":   len(list),
	})
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	role := r.URL.Query().Get("role")
	deviceID := r.URL.Query().Get("device")

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	if role == "device" || deviceID == "" {
		handleDevice(conn)
	} else {
		handleParent(conn, deviceID)
	}
}

func handleDevice(conn *websocket.Conn) {
	defer conn.Close()

	_, msg, err := conn.ReadMessage()
	if err != nil {
		log.Printf("Read device register failed: %v", err)
		return
	}

	var registerMsg Message
	if err := json.Unmarshal(msg, &registerMsg); err != nil {
		log.Printf("Parse register failed: %v", err)
		return
	}

	if registerMsg.Type != "register" || registerMsg.DeviceID == "" {
		log.Printf("Invalid register message")
		return
	}

	deviceID := registerMsg.DeviceID
	log.Printf("Device registered: %s", deviceID)

	dev := &Device{
		ID:       deviceID,
		Conn:     conn,
		Parents:  make(map[*websocket.Conn]bool),
		LastSeen: time.Now(),
	}

	devicesMu.Lock()
	if old, exists := devices[deviceID]; exists {
		old.Conn.Close()
	}
	devices[deviceID] = dev
	devicesMu.Unlock()

	defer func() {
		devicesMu.Lock()
		delete(devices, deviceID)
		devicesMu.Unlock()
		log.Printf("Device disconnected: %s", deviceID)
	}()

	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		dev.LastSeen = time.Now()
		dev.ParentsMu.RLock()
		for parent := range dev.Parents {
			if err := parent.WriteMessage(messageType, data); err != nil {
				dev.ParentsMu.RUnlock()
				dev.ParentsMu.Lock()
				delete(dev.Parents, parent)
				dev.ParentsMu.Unlock()
				dev.ParentsMu.RLock()
			}
		}
		dev.ParentsMu.RUnlock()
	}
}

func handleParent(conn *websocket.Conn, deviceID string) {
	defer conn.Close()
	log.Printf("Parent connected, device: %s", deviceID)

	devicesMu.RLock()
	dev, exists := devices[deviceID]
	devicesMu.RUnlock()

	if !exists {
		conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"Device offline"}`))
		return
	}

	dev.ParentsMu.Lock()
	dev.Parents[conn] = true
	dev.ParentsMu.Unlock()

	defer func() {
		dev.ParentsMu.Lock()
		delete(dev.Parents, conn)
		dev.ParentsMu.Unlock()
	}()

	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if err := dev.Conn.WriteMessage(messageType, data); err != nil {
			break
		}
	}
}

const deviceListHTML = `
<!doctype html>
<html><head><meta charset="utf-8"><title>Device List</title>
<style>body{background:#0a0a0f;color:#fff;font-family:system-ui;padding:20px}
h1{color:#667eea}.device{background:#1a1a2e;padding:16px;border-radius:8px;margin-bottom:12px;cursor:pointer}
.device:hover{background:#252545}.id{font-size:18px;font-weight:bold}.info{color:#888;font-size:13px;margin-top:4px}</style>
</head><body>
<h1>Online Devices</h1>
<div id="list"><p>Loading...</p></div>
<script>
function load(){
  fetch('/api/devices').then(r=>r.json()).then(d=>{
    if(d.devices.length===0){document.getElementById('list').innerHTML='<p>No devices online</p>';return;}
    document.getElementById('list').innerHTML=d.devices.map(dev=>
      '<div class="device" onclick="location.href=\'/?device='+dev.id+'\'">'+
      '<div class="id">'+dev.id+'</div>'+
      '<div class="info">'+dev.parents+' parent(s) online</div></div>'
    ).join('');
  });
}
load();setInterval(load,5000);
</script>
</body></html>
`

const parentHTML = `
<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Remote Monitor - %s</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#000;color:#fff;font-family:system-ui;height:100vh;display:flex;flex-direction:column}
#status{background:#1a1a2e;padding:10px;text-align:center;font-size:14px}
#screen{flex:1;display:flex;align-items:center;justify-content:center;overflow:hidden}
#screen img{max-width:100%;max-height:100%;object-fit:contain}
#controls{background:#1a1a2e;padding:10px;display:flex;gap:8px;justify-content:center;flex-wrap:wrap}
button{padding:10px 16px;background:#667eea;border:none;border-radius:8px;color:#fff;cursor:pointer}
button:active{transform:scale(.95)}
</style></head><body>
<div id="status">Connecting...</div>
<div id="screen"><img id="img" alt="screen"></div>
<div id="controls">
  <button onclick="send('home')">Home</button>
  <button onclick="send('back')">Back</button>
  <button onclick="send('lock')">Lock</button>
  <button onclick="send('screenshot')">Screenshot</button>
</div>
<script>
const deviceID='%s';
const img=document.getElementById('img');
const status=document.getElementById('status');
let ws;
function connect(){
  const proto=location.protocol==='https:'?'wss':'ws';
  ws=new WebSocket(proto+'://'+location.host+'/ws?role=parent&device='+deviceID);
  ws.binaryType='blob';
  ws.onopen=()=>{status.textContent='Connected - '+deviceID;status.style.color='#76ff9f';};
  ws.onmessage=e=>{
    if(e.data instanceof Blob){
      const url=URL.createObjectURL(e.data);
      img.src=url;
      img.onload=()=>URL.revokeObjectURL(url);
    }else{
      try{const d=JSON.parse(e.data);if(d.type==='error'){status.textContent='Error: '+d.message;status.style.color='#f5576c';}}catch(e){}
    }
  };
  ws.onclose=()=>{status.textContent='Reconnecting...';status.style.color='#ff9f43';setTimeout(connect,1000);};
}
connect();
function send(action){ws.send(JSON.stringify({type:'command',action:action}));}
let touchX=0,touchY=0;
img.addEventListener('touchstart',e=>{
  e.preventDefault();
  const t=e.touches[0];
  const r=img.getBoundingClientRect();
  touchX=Math.round((t.clientX-r.left)/r.width*img.naturalWidth);
  touchY=Math.round((t.clientY-r.top)/r.height*img.naturalHeight);
});
img.addEventListener('touchend',e=>{
  e.preventDefault();
  ws.send(JSON.stringify({type:'touch',action:'tap',x:touchX,y:touchY}));
});
</script>
</body></html>
`
