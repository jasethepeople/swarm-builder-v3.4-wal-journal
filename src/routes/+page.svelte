
<script lang="ts">
import { onMount } from 'svelte';
import { agents, peers, meshStatus, nodeId, hashLog, initMesh, addAgent, broadcastTask } from '$lib/stores/swarm.svelte.ts';
import { connect } from '$lib/transport/mesh.ts';
import { invoke } from '@tauri-apps/api/core';
import { open } from '@tauri-apps/plugin-dialog';
import { listen } from '@tauri-apps/api/event';
let discovered:Record<string,string>={}; let raft:any={members:[], old_members:null, role:'follower', term:0, commit_index:0, is_leader:false}; let raftEntries:any[]=[]; let raftLog:string[]=[]; let taskInput='analyze market'; let newMemberInput=''; let snapshot:any=null; let mdnsStatus='off'; let secretInput=''; let secretInfo=''; let hasSecret=false; let importStatus=''; let selectedModel='base'; let models:string[]=[]; let threshold=20; let autoMembers=true; let autoSave=true; let lastAutoSave=0; let persistedList:string[]=[]; let persistStatus=''; let walStats:any={}; let walEntries:any[]=[];
onMount(async()=>{
  await initMesh();
  await invoke('raft_init',{nodeId});
  await invoke('consensus_set_node_id',{nodeId});
  await listen('discovery:peer-found', async (e:any)=>{
    const {node_id, address}=e.payload;
    if(node_id!==nodeId){
      discovered[node_id]=address;
      if(autoMembers){
        // auto-connect new mDNS nodes
        try{ await connect(node_id, address); peers[node_id]=address; }catch{}
        // if leader and new node not in members, auto propose add
        if(raft.is_leader && !raft.members.includes(node_id)){
          try{
            const r=await invoke('raft_add_member',{newMember: node_id});
            raftLog.unshift(r as string);
            setTimeout(async()=>{ try{ const f=await invoke('raft_finalize_membership'); raftLog.unshift(f as string); }catch(err){ raftLog.unshift(`finalize fail ${err}`);} }, 2000);
          }catch{}
        }
      }
    }
  });
  await listen('raft:heartbeat', (e:any)=>{ raftLog.unshift(`♥ ${e.payload.leader} term ${e.payload.term}`); });
  await listen('raft:election', (e:any)=>{ raftLog.unshift(`🗳️ election ${e.payload.candidate} term ${e.payload.term}`); });
  await listen('raft:elected', (e:any)=>{ raftLog.unshift(`👑 LEADER ${e.payload.leader} term ${e.payload.term}`); });
  await listen('raft:membership', (e:any)=>{ raftLog.unshift(`👥 membership ${e.payload.type} ${e.payload.action||''} ${e.payload.member||''} -> ${JSON.stringify(e.payload.new)} idx ${e.payload.index}`); });
  await listen('raft:log_append', (e:any)=>{ raftLog.unshift(`📝 append #${e.payload.index} ${e.payload.hash.slice(0,8)}`); });
  await listen('raft:committed', (e:any)=>{ raftLog.unshift(`✓ commit ${e.payload.commit_index}`); });
  await listen('raft:snapshot', (e:any)=>{ raftLog.unshift(`📸 snapshot #${e.payload.index} ${e.payload.hash.slice(0,8)}`); });
  await listen('whisper:download-progress', (e:any)=>{ importStatus=`${e.payload.pct}% ${e.payload.url}`; });
  try{ mdnsStatus=await invoke('discovery_start',{nodeId, port:5556}); }catch(err){ mdnsStatus=String(err); }
  setInterval(async()=>{
    if(autoSave){
      try{
        const now=Date.now();
        if(now-lastAutoSave>5000){
          const state=await invoke('raft_state');
          const full={...state as object, log: await invoke('raft_log').catch(()=>[]), snapshot: await invoke('raft_snapshot_info').catch(()=>null), node_id: nodeId};
          const r=await invoke('persistence_save',{nodeId, state: full});
          persistStatus=`auto-saved ${new Date().toLocaleTimeString()} ${ (r as string).slice(0,60)}`;
          lastAutoSave=now;
        }
      }catch(e){}
    }

    discovered=await invoke('discovery_peers').catch(()=>discovered);
    raft=await invoke('raft_state').catch(()=>raft);
    raftEntries=await invoke('raft_log').catch(()=>raftEntries);
    snapshot=await invoke('raft_snapshot_info').catch(()=>snapshot);
    models=await invoke('whisper_list_models').catch(()=>models);
    persistedList=await invoke('persistence_list').catch(()=>persistedList);
    try{ walStats=await invoke('wal_stats',{nodeId}); walEntries=await invoke('wal_read',{nodeId, limit: 20}); }catch{}

    try{ const saved=await invoke('persistence_load',{nodeId}); const restored=await invoke('raft_restore_from_persisted',{persisted:saved}); raftLog.unshift(restored as string); }catch{}
    .catch(()=>models);
    const tick=await invoke('raft_tick').catch(()=>null);
    if(tick) raftLog.unshift(tick as string);
    if(raft.is_leader){
      const auto=await invoke('crypto_check_auto_rotate').catch(()=>null);
      if(auto) secretInfo=auto as string;
    }
  }, 900);
});
async function append(){ try{ const e=await invoke('raft_append_task',{task:taskInput}) as any; raftLog.unshift(`appended #${e.index}`); setTimeout(async()=>{ await invoke('raft_commit',{index:e.index}); }, 400); }catch(err){ raftLog.unshift(`append fail ${err}`); } }
async function addMember(){ try{ const r=await invoke('raft_add_member',{newMember:newMemberInput}); raftLog.unshift(r as string); newMemberInput=''; setTimeout(async()=>{ const f=await invoke('raft_finalize_membership'); raftLog.unshift(f as string); }, 1500); }catch(e){ raftLog.unshift(`add fail ${e}`); } }
async function setSecret(){ secretInfo=await invoke('crypto_set_secret',{secret:secretInput}) as string; hasSecret=true; }
</script>

<div class="min-h-screen bg-zinc-950 text-zinc-100 p-3 grid grid-cols-[1fr_540px] gap-3 text-xs">
  <div class="border border-zinc-800 rounded-xl bg-zinc-900/50 flex flex-col">
    <div class="flex gap-2 p-3 border-b border-zinc-800 items-center">
      <button on:click={addAgent} class="px-3 py-1 bg-white text-black rounded">+ Agent</button>
      <input bind:value={taskInput} class="bg-zinc-800 px-2 py-1 rounded w-56" placeholder="task to replicate"/>
      <button on:click={append} class="px-3 py-1 bg-violet-600 rounded disabled:opacity-50" disabled={!raft.is_leader}>Append Task {raft.is_leader?'👑':''}</button>
      <span class="ml-auto text-[11px] text-zinc-400">{raft.role} term {raft.term} commit {raft.commit_index} | {meshStatus}</span>
    </div>
    {#if raft.old_members}<div class="p-2 bg-amber-950/40 border-b border-amber-900/30 text-[11px]">🔄 JOINT CONSENSUS IN PROGRESS old {JSON.stringify(raft.old_members)} -> new {JSON.stringify(raft.members)} waiting commit {raft.joint_index} then finalize</div>{/if}
    {#if snapshot}<div class="p-2 bg-zinc-800/50 border-b border-zinc-700 text-[11px]">📸 Snapshot up to #{snapshot.last_included_index} term {snapshot.last_included_term} blake3 {snapshot.blake3_hash?.slice(0,16)} members {JSON.stringify(snapshot.membership)} | Log after: {raftEntries.length}</div>{/if}
    <div class="p-2 border-b border-zinc-800 flex gap-2 flex-wrap">
      <div class="font-bold">Members ({raft.members?.length||0}) {autoMembers?'auto-join on':''}</div>
      {#each raft.members||[] as m}<div class="px-2 py-1 rounded text-[11px] flex items-center gap-2 {m===nodeId?'bg-violet-900/50 border border-violet-700':'bg-zinc-800'}">{m} {m===raft.leader_id?'👑':''} {#if raft.is_leader && m!==nodeId}<button on:click={async()=>{ try{ const r=await invoke('raft_remove_member',{member:m}); raftLog.unshift(r as string); setTimeout(async()=>{ const f=await invoke('raft_finalize_membership'); raftLog.unshift(f as string); }, 1500); }catch(e){ raftLog.unshift(`remove fail ${e}`); } }} class="px-1 bg-red-900/60 rounded">x</button>{/if}</div>{/each}
      <label class="flex items-center gap-1 ml-2"><input type="checkbox" bind:checked={autoMembers}/> auto-add mDNS</label>
    </div>
    <div class="p-3 grid grid-cols-3 gap-2 overflow-auto">{#each raftEntries as e}<div class="p-2 rounded border {e.committed?'border-emerald-700 bg-emerald-950/20':'border-zinc-700 bg-zinc-800'}"><div class="font-bold flex justify-between">#{e.index} {e.entry_type} {e.committed?'✓':''} <span class="text-[9px]">{e.term}</span></div><div class="text-[11px] mt-1 truncate">{e.task.slice(0,120)}</div><div class="text-[9px] font-mono text-zinc-500 mt-1">{e.blake3_hash.slice(0,16)}</div></div>{/each}</div>
    <div class="p-3 border-t border-zinc-800 h-56 overflow-auto"><div class="font-bold mb-1">Raft Log & Membership</div>{#each raftLog as l}<div class="font-mono text-[10px] text-zinc-500">{l}</div>{/each}</div>
  </div>

  <div class="space-y-3">
    <div class="border border-emerald-900/50 bg-emerald-950/20 rounded-xl p-3">
      <h3 class="font-bold text-emerald-200">👥 MEMBERSHIP - Joint Consensus + mDNS Auto-Join</h3>
      <div class="text-[10px] text-zinc-400">New mDNS nodes auto-connect and if leader, auto-propose add via joint consensus (old+new need quorum) -> finalize. Prevents split brain.</div>
      <div class="flex gap-2 mt-3"><input bind:value={newMemberInput} placeholder="new node id e.g. node-xyz" class="flex-1 bg-zinc-800 rounded px-2 py-1"/><button on:click={addMember} class="px-3 py-1 bg-emerald-700 rounded disabled:opacity-50" disabled={!raft.is_leader}>Add Member {raft.is_leader?'👑':'(need leader)'}</button></div>
      <button on:click={async()=>{ const r=await invoke('raft_finalize_membership'); raftLog.unshift(r as string); }} class="w-full mt-2 bg-zinc-800 rounded py-1">Finalize Joint -> New Config</button>
      <div class="mt-2 text-[10px] text-zinc-500">Joint index {raft.joint_index} | Old {JSON.stringify(raft.old_members)} | Current {JSON.stringify(raft.members)}</div>
    </div>

    <div class="border border-zinc-800 rounded-xl p-3 bg-zinc-900">
      <h3 class="font-bold">MESH ROUTER P2P + mDNS Discovery {hasSecret?'🔒':''}</h3>
      <div class="text-[10px] text-zinc-500">mDNS: {mdnsStatus} | ID {nodeId} {raft.leader_id===nodeId?'👑 leader':''}</div>
      <div class="mt-2 text-[11px] font-bold">Discovered (auto-connect {autoMembers?'on':'off'})</div>
      {#each Object.entries(discovered) as [id,addr]}<div class="flex justify-between py-1 border-b border-zinc-800 text-[11px]"><div><div>{id} {raft.members?.includes(id)?'member':''}</div><div class="text-[9px] text-zinc-500">{addr}</div></div><button on:click={async()=>{ await connect(id,addr); peers[id]=addr; }} class="px-2 bg-white text-black rounded text-[10px]">Connect</button></div>{:else}<div class="text-[10px] text-zinc-600 py-2">No LAN peers - start another instance on same WiFi</div>{/each}
      <div class="mt-3 text-[11px]">Connected peers</div>
      {#each Object.entries(peers) as [id,addr]}<div class="text-[10px] text-zinc-400">{id} -> {addr}</div>{/each}
    </div>

    <div class="border border-amber-900/30 bg-amber-950/10 rounded-xl p-3">
      <h3 class="font-bold">🔐 Encryption + Snapshot</h3>
      <input bind:value={secretInput} type="password" placeholder="master secret" class="w-full bg-zinc-800 rounded px-2 py-1 mt-1"/>
      <button on:click={setSecret} class="w-full bg-amber-200 text-black rounded py-1 mt-1 font-bold">Set Secret</button>
      <div class="text-[10px] mt-1 text-amber-200/60">{secretInfo} {hasSecret?'🔒 encrypted mesh':''}</div>
      <div class="flex gap-2 mt-2"><input type="number" bind:value={threshold} class="w-20 bg-zinc-800 rounded px-2 py-1"/><button on:click={async()=>{ await invoke('raft_set_snapshot_threshold',{threshold}); }} class="px-2 bg-zinc-700 rounded">Set thresh</button><button on:click={async()=>{ const s=await invoke('raft_create_snapshot'); raftLog.unshift(`snapshot ${(s as any).last_included_index}`); }} class="px-2 bg-amber-800 rounded">Snapshot Now</button></div>
    </div>

    <div class="border border-zinc-800 rounded-xl p-3 bg-zinc-900">
      <h3 class="font-bold">Voice whisper.cpp + blake3</h3>
      <select bind:value={selectedModel} class="w-full bg-zinc-800 rounded px-2 py-1 mt-1"><option value="tiny">tiny 75MB</option><option value="base">base 142MB</option><option value="small">small 466MB</option></select>
      <div class="grid grid-cols-2 gap-2 mt-2"><button on:click={async()=>{ const {downloadModel}=await import('$lib/voice/whisper.ts'); await downloadModel(selectedModel, ()=>{}); }} class="bg-white text-black rounded py-1">Download</button><button on:click={async()=>{ const f=await open({multiple:false}); if(f){ const r=await invoke('whisper_import_model',{sourcePath:f, model:selectedModel}); importStatus=r as string; } }} class="bg-zinc-700 rounded py-1">Import .bin</button></div>
      <div class="text-[10px] mt-1">{importStatus}</div>
    </div>

    <div class="border border-zinc-800 rounded-xl p-3 bg-zinc-900">
      <h3 class="font-bold">📓 WAL Journal - zero loss</h3>
      <div class="text-[10px] text-zinc-400">Every append writes blake3 verified entry immediately</div>
      <div class="text-[10px] mt-1">Entries {walStats.entries||0} | {((walStats.size_bytes||0)/1024).toFixed(1)}KB | Last #{walStats.last_seq||0}</div>
      <div class="flex gap-2 mt-2"><button on:click={async()=>{ walEntries=await invoke('wal_read',{nodeId, limit: 50}); walStats=await invoke('wal_stats',{nodeId}); }} class="px-2 py-1 bg-zinc-700 rounded">Read</button><button on:click={async()=>{ const r=await invoke('wal_replay',{nodeId}); raftLog.unshift(r as string); }} class="px-2 py-1 bg-white text-black rounded">Replay</button><button on:click={async()=>{ await invoke('wal_truncate',{nodeId, keepLast: 20}); walStats=await invoke('wal_stats',{nodeId}); }} class="px-2 py-1 bg-zinc-700 rounded">Truncate</button><button on:click={async()=>{ await invoke('wal_delete',{nodeId}); walStats=await invoke('wal_stats',{nodeId}); }} class="px-2 py-1 bg-red-900 rounded">Delete</button></div>
      <div class="mt-2 max-h-32 overflow-auto">{#each walEntries.slice().reverse() as w}<div class="text-[9px] font-mono text-zinc-500">#{w.seq} {w.op} {w.blake3_hash?.slice(0,8)} {JSON.stringify(w.data).slice(0,50)}</div>{/each}</div>
    </div>
    <div class="border border-zinc-800 rounded-xl p-3 bg-zinc-900">
      <h3 class="font-bold">💾 Persistence - survives restart</h3>
      <div class="text-[10px] text-zinc-400">Saves term, voted_for, members, log, commit_index, snapshot with blake3 integrity | Auto-save every 5s to data_dir/swarm-builder/raft/</div>
      <div class="flex items-center gap-2 mt-2"><label class="flex items-center gap-1 text-[11px]"><input type="checkbox" bind:checked={autoSave}/> auto-save 5s</label><button on:click={async()=>{ const state=await invoke('raft_state'); const snap=await invoke('raft_snapshot_info'); const full={...state as object, log: await invoke('raft_log'), snapshot: snap, node_id: nodeId}; const r=await invoke('persistence_save',{nodeId, state: full}); persistStatus=r as string; persistedList=await invoke('persistence_list'); }} class="px-3 py-1 bg-zinc-700 rounded">Save State</button><button on:click={async()=>{ try{ const saved=await invoke('persistence_load',{nodeId}); const r=await invoke('raft_restore_from_persisted',{persisted:saved}); persistStatus=r as string; }catch(e){ persistStatus=String(e); } }} class="px-3 py-1 bg-white text-black rounded">Restore</button><button on:click={async()=>{ await invoke('persistence_delete',{nodeId}); persistStatus=`deleted ${nodeId}`; }} class="px-3 py-1 bg-red-900 rounded">Delete</button></div>
      <div class="text-[10px] mt-1 text-zinc-500">{persistStatus} | files: {persistedList.join(', ')}</div>
    </div>
    <div class="border border-zinc-800 rounded-xl p-3 bg-zinc-900 flex gap-2">
      <button on:click={async()=>{ await invoke('raft_start_election'); }} class="px-3 py-1 bg-violet-700 rounded">Election</button>
      <button on:click={async()=>{ await invoke('raft_step_down'); }} class="px-3 py-1 bg-zinc-700 rounded">Step Down</button>
      <span class="text-[10px] text-zinc-500 ml-auto">blake3 hashes everywhere</span>
    </div>
  </div>
</div>
