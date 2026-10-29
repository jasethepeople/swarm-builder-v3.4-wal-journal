
#[tauri::command] pub fn consensus_set_node_id(node_id:String)->String{node_id}
#[tauri::command] pub fn consensus_status()->Vec<serde_json::Value>{vec![]}
#[tauri::command] pub fn consensus_propose_rotation(version:u64,new_secret:String,node_id:String)->String{format!("propose v{}",version)}
#[tauri::command] pub fn consensus_vote(version:u64,node_id:String)->String{format!("vote v{} {}",version,node_id)}
#[tauri::command] pub fn consensus_handle_broadcast(payload:String)->Option<String>{Some(payload)}
