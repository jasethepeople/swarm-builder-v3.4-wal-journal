
use std::collections::HashMap;
use std::sync::{Arc,Mutex,LazyLock};
use tauri::{Emitter, Window};
use mdns_sd::{ServiceDaemon, ServiceInfo, ServiceEvent};
static MDNS: LazyLock<Mutex<Option<ServiceDaemon>>>=LazyLock::new(|| Mutex::new(None));
static DISCOVERED: LazyLock<Arc<Mutex<HashMap<String,String>>>>=LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));
#[tauri::command]
pub fn discovery_start(window: Window, node_id: String, port: u16)->Result<String,String>{
  let mdns = ServiceDaemon::new().map_err(|e| e.to_string())?;
  let service_type = "_swarm._tcp.local.";
  let instance_name = format!("swarm-{}", node_id);
  let host_name = format!("{}.local.", node_id);
  let props = HashMap::from([("node_id".to_string(), node_id.clone()), ("port".to_string(), port.to_string())]);
  let service_info = ServiceInfo::new(service_type, &instance_name, &host_name, "", port, props).map_err(|e| e.to_string())?;
  mdns.register(service_info).map_err(|e| e.to_string())?;
  let browse_mdns = mdns.clone();
  let win = window.clone();
  let discovered = DISCOVERED.clone();
  std::thread::spawn(move || {
    let receiver = browse_mdns.browse(service_type).unwrap();
    while let Ok(event) = receiver.recv() {
      match event {
        ServiceEvent::ServiceResolved(info) => {
          let id = info.get_property_val_str("node_id").unwrap_or("unknown").to_string();
          let port = info.get_port();
          let ip = info.get_addresses().iter().next().map(|a| a.to_string()).unwrap_or_default();
          let addr = format!("tcp://{}:{}", ip, port);
          discovered.lock().unwrap().insert(id.clone(), addr.clone());
          let _ = win.emit("discovery:peer-found", serde_json::json!({"node_id": id, "address": addr}));
        },
        ServiceEvent::ServiceRemoved(_, fullname) => { let _ = win.emit("discovery:peer-lost", serde_json::json!({"fullname": fullname})); },
        _ => {}
      }
    }
  });
  *MDNS.lock().unwrap()=Some(mdns);
  Ok(format!("mDNS swarm-{} on {}", node_id, port))
}
#[tauri::command] pub fn discovery_peers()->HashMap<String,String>{DISCOVERED.lock().unwrap().clone()}
#[tauri::command] pub fn discovery_stop()->Result<String,String>{*MDNS.lock().unwrap()=None; Ok("stopped".into())}
