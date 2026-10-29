
use std::collections::HashMap;
use std::sync::{Arc,Mutex,LazyLock};
use zmq;
static CTX: LazyLock<zmq::Context>=LazyLock::new(|| zmq::Context::new());
static PEERS: LazyLock<Arc<Mutex<HashMap<String,String>>>>=LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));
static ROUTER: LazyLock<Mutex<Option<zmq::Socket>>>=LazyLock::new(|| Mutex::new(None));
#[tauri::command] pub fn mesh_listen(port:u16,node_id:String)->Result<String,String>{let s=CTX.socket(zmq::ROUTER).map_err(|e|e.to_string())?; s.set_identity(node_id.as_bytes()).map_err(|e|e.to_string())?; s.bind(&format!("tcp://0.0.0.0:{}",port)).map_err(|e|e.to_string())?; *ROUTER.lock().unwrap()=Some(s); Ok(format!("ROUTER {} on {}",node_id,port))}
#[tauri::command] pub fn mesh_connect(peer_id:String,address:String)->Result<String,String>{PEERS.lock().unwrap().insert(peer_id.clone(),address.clone()); Ok(format!("{}->{}",peer_id,address))}
#[tauri::command] pub fn mesh_peers()->HashMap<String,String>{PEERS.lock().unwrap().clone()}
#[tauri::command] pub fn mesh_disconnect(peer_id:String)->Result<String,String>{PEERS.lock().unwrap().remove(&peer_id); Ok(peer_id)}
#[tauri::command] pub fn mesh_broadcast(payload:String)->Result<String,String>{let h=blake3::hash(payload.as_bytes()).to_hex().to_string(); if let Some(sock)=ROUTER.lock().unwrap().as_ref(){ for (pid,_) in PEERS.lock().unwrap().clone(){ let _=sock.send(&pid,zmq::SNDMORE); let _=sock.send(&h,zmq::SNDMORE); let _=sock.send(&payload,0);} } Ok(h)}
#[tauri::command] pub fn mesh_send(peer_id:String,payload:String)->Result<String,String>{let h=blake3::hash(payload.as_bytes()).to_hex().to_string(); if let Some(sock)=ROUTER.lock().unwrap().as_ref(){ sock.send(&peer_id,zmq::SNDMORE).map_err(|e|e.to_string())?; sock.send(&h,zmq::SNDMORE).map_err(|e|e.to_string())?; sock.send(&payload,0).map_err(|e|e.to_string())?; } Ok(h)}
