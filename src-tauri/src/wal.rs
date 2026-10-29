
use std::path::PathBuf;
use std::io::{Write, BufRead, BufReader};
use serde::{Serialize, Deserialize};
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WALEntry { pub seq: u64, pub timestamp: u64, pub node_id: String, pub op: String, pub data: serde_json::Value, pub blake3_hash: String, }
fn wal_dir()->PathBuf{ dirs::data_dir().unwrap_or_else(|| PathBuf::from(".")).join("swarm-builder").join("raft").join("wal") }
fn wal_path(node_id: &str)->PathBuf{ wal_dir().join(format!("wal-{}.log", node_id)) }
#[tauri::command]
pub fn wal_append(node_id: String, op: String, data: serde_json::Value)->Result<String,String>{
  let dir = wal_dir(); std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?; let path = wal_path(&node_id);
  let seq = if path.exists(){ std::fs::read_to_string(&path).unwrap_or_default().lines().count() as u64 + 1 } else { 1 };
  let payload = format!("{}:{}:{}:{}:{}", seq, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis(), node_id, op, serde_json::to_string(&data).unwrap_or_default()); let blake3_hash = blake3::hash(payload.as_bytes()).to_hex().to_string();
  let entry = WALEntry{ seq, timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64, node_id: node_id.clone(), op: op.clone(), data, blake3_hash: blake3_hash.clone() };
  let line = serde_json::to_string(&entry).map_err(|e| e.to_string())?; let mut file = std::fs::OpenOptions::new().create(true).append(true).open(&path).map_err(|e| e.to_string())?; writeln!(file, "{}", line).map_err(|e| e.to_string())?; Ok(format!("WAL #{} {}", seq, &blake3_hash[..12]))
}
#[tauri::command]
pub fn wal_read(node_id: String, limit: Option<usize>)->Result<Vec<WALEntry>,String>{
  let path = wal_path(&node_id); if !path.exists(){ return Ok(vec![]); }
  let file = std::fs::File::open(&path).map_err(|e| e.to_string())?; let reader = BufReader::new(file); let mut entries = Vec::new();
  for line in reader.lines(){ let line = line.map_err(|e| e.to_string())?; if line.trim().is_empty(){ continue; } if let Ok(entry) = serde_json::from_str::<WALEntry>(&line){ entries.push(entry); } }
  if let Some(lim) = limit { if entries.len() > lim { entries = entries[entries.len()-lim..].to_vec(); } } Ok(entries)
}
#[tauri::command] pub fn wal_replay(node_id: String)->Result<String,String>{ let entries = wal_read(node_id.clone(), None)?; Ok(format!("replayed {} WAL entries", entries.len())) }
#[tauri::command]
pub fn wal_truncate(node_id: String, keep_last: usize)->Result<String,String>{ let entries = wal_read(node_id.clone(), None)?; if entries.len() <= keep_last { return Ok(format!("len {}", entries.len())); } let to_keep = entries[entries.len()-keep_last..].to_vec(); let path = wal_path(&node_id); let mut file = std::fs::File::create(&path).map_err(|e| e.to_string())?; for entry in to_keep { let line = serde_json::to_string(&entry).map_err(|e| e.to_string())?; writeln!(file, "{}", line).map_err(|e| e.to_string())?; } Ok(format!("truncated to {}", keep_last)) }
#[tauri::command] pub fn wal_stats(node_id: String)->Result<serde_json::Value,String>{ let entries = wal_read(node_id.clone(), None)?; let path = wal_path(&node_id); let size = if path.exists(){ std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0) } else { 0 }; Ok(serde_json::json!({"entries": entries.len(), "size_bytes": size, "last_seq": entries.last().map(|e| e.seq).unwrap_or(0)})) }
#[tauri::command] pub fn wal_delete(node_id: String)->Result<String,String>{ let path = wal_path(&node_id); let _ = std::fs::remove_file(&path); Ok(format!("deleted {}", node_id)) }
