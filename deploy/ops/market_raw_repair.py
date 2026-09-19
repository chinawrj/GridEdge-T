#!/usr/bin/env python3
"""Repair only a proven missing committed interval while the worker is quiescent.

This module has no order API or MQTT publisher. PostgreSQL is read-only. Known
local records are never rewritten, removed or reordered, including duplicates.
"""
from datetime import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import stat
import sys
from zoneinfo import ZoneInfo

TZ=ZoneInfo('Asia/Shanghai')
SOURCE='eastmoney-web-time-sales'
INSTANCE='8101d65c-bdba-4de3-83e0-8983506f159e'

def validator():
    path=Path(__file__).with_name('market_ingestor.py')
    if not path.exists():path=Path(__file__).parents[1]/'market_data/ingestor/market_ingestor.py'
    spec=importlib.util.spec_from_file_location('raw_repair_validation',path)
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module
    spec.loader.exec_module(module)
    return module.validate_document

VALIDATE=validator()

def digest(value):return hashlib.sha256(value).hexdigest()

def decode(topic,payload,cutoff):
    event=VALIDATE(payload,topic)
    if (event.source_id!=SOURCE or str(event.source_instance_id)!=INSTANCE or
        event.venue!='XSHE' or event.symbol!='002256' or
        event.event_type not in ('TRADE_TICK','SOURCE_STATUS')):
        raise ValueError('unreviewed raw market identity')
    if max(event.ts_us,event.recv_us)>cutoff:
        raise ValueError('future raw market event')
    return event

def local_records(original,cutoff):
    if not original or not original.endswith(b'\n'):
        raise ValueError('raw is empty or contains an incomplete tail')
    records=[];previous=None;seen={}
    for line in original.splitlines(keepends=True):
        envelope=json.loads(line)
        if set(envelope)!= {'topic','payload'}:raise ValueError('unknown raw envelope')
        payload=bytes(envelope['payload']);topic=envelope['topic']
        event=decode(topic,payload,cutoff);seq=event.source_sequence
        if previous is not None and seq<previous and seq not in seen:raise ValueError('local raw is reordered')
        if seq in seen and seen[seq]!=(topic,payload):raise ValueError('conflicting local duplicate')
        seen[seq]=(topic,payload);previous=max(seq,previous if previous is not None else seq)
        records.append((seq,topic,payload,line))
    return records

def missing_intervals(original,cutoff):
    records=local_records(original,cutoff)
    sequences=list(dict.fromkeys(record[0] for record in records))
    return [(a+1,b-1) for a,b in zip(sequences,sequences[1:]) if b>a+1]

def prepare(original,rows,cutoff):
    records=local_records(original,cutoff)
    low,high=records[0][0],max(record[0] for record in records)
    if high-low>1000000:raise ValueError('repair interval exceeds bounded contract')
    committed={};previous=low-1
    for row in rows:
        payload=bytes.fromhex(row['payload_hex']);event=decode(row['topic'],payload,cutoff)
        seq=event.source_sequence
        if (seq!=previous+1 or not low<=seq<=high or row['source_sequence']!=seq or
            row['payload_sha256']!=digest(payload) or row['event_id']!=event.event_id.hex() or
            row['qos']!=1):raise ValueError('committed range identity or continuity differs')
        committed[seq]=(row['topic'],payload);previous=seq
    if previous!=high:raise ValueError('committed range is incomplete')
    output=[];added=[];previous=low-1
    for seq,topic,payload,line in records:
        if committed.get(seq)!=(topic,payload):raise ValueError('known local record conflicts with committed bytes')
        for missing in range(previous+1,seq):
            committed_topic,committed_payload=committed[missing]
            output.append(json.dumps(dict(topic=committed_topic,payload=list(committed_payload)),separators=(',',':')).encode()+b'\n')
            added.append(missing)
        output.append(line);previous=max(previous,seq)
    candidate=b''.join(output)
    return candidate,dict(schema='gridedge.raw-repair.v1',source_id=SOURCE,source_instance_id=INSTANCE,
        original_sha256=digest(original),candidate_sha256=digest(candidate),low=low,high=high,
        added_sequences=added,cutoff_us=cutoff,money_actions_enabled=False)

def committed_rows(low,high):
    if not (isinstance(low,int) and isinstance(high,int) and 0<=low<=high and high-low<=1000000):
        raise ValueError('invalid bounded committed interval')
    query=("SELECT json_build_object('topic',mqtt_topic,'payload_hex',encode(payload_bytes,'hex'),"
        "'event_id',encode(event_id,'hex'),'payload_sha256',encode(payload_sha256,'hex'),"
        "'source_sequence',source_sequence,'qos',qos)::text FROM market_events WHERE "
        f"source_id='{SOURCE}' AND source_instance_id='{INSTANCE}' "
        f"AND source_sequence BETWEEN {low} AND {high} ORDER BY source_sequence")
    remote=shlex.join(['/usr/local/bin/docker','exec','gridedge-market-postgres','psql',
        '-U','gridedge_market','-d','gridedge_market','-Atqc',query])
    result=subprocess.run(['/usr/bin/ssh','-o','BatchMode=yes','-o','ConnectTimeout=3',
        '-o','LogLevel=ERROR','192.168.1.201',remote],check=True,timeout=45,capture_output=True)
    return [json.loads(line) for line in result.stdout.splitlines()]

def day_inputs(candidate):
    days={}
    for line in candidate.splitlines(keepends=True):
        event=json.loads(bytes(json.loads(line)['payload']))
        day=datetime.fromtimestamp(event['ts_us']/1000000,TZ).date().isoformat()
        days.setdefault(day,[]).append(line)
    return {day:b''.join(lines) for day,lines in days.items()}

def semantic_replay(candidate,directory,replay_binary):
    days=day_inputs(candidate)
    results=[]
    binary_sha256=digest(Path(replay_binary).read_bytes())
    for day,payload in sorted(days.items()):
        source=directory/(day+'.jsonl');source.write_bytes(payload)
        output=directory/(day+'-replay.json')
        subprocess.run([str(replay_binary),'--input',str(source),'--output',str(output),
            '--symbol','002256.SZ','--session-date',day,'--allow-partial-session-resume-boundary'],
            check=True,timeout=60,capture_output=True)
        if digest(Path(replay_binary).read_bytes())!=binary_sha256:raise ValueError('semantic replay binary changed')
        results.append(dict(day=day,input_sha256=digest(source.read_bytes()),output_sha256=digest(output.read_bytes()),
                            replay_binary_sha256=binary_sha256,exit_code=0))
    return results

def fsync_directory(path):
    fd=os.open(path,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)

def publish(path,original,candidate,audit,verify_quiescence):
    """Caller owns coordination + maintenance; recheck immediately before replace."""
    path=Path(path);verify_quiescence()
    if digest(original)!=audit['original_sha256'] or digest(candidate)!=audit['candidate_sha256']:
        raise ValueError('raw repair audit byte identity differs')
    if path.is_symlink() or path.read_bytes()!=original:raise ValueError('raw changed before publication')
    expected={day:digest(payload) for day,payload in day_inputs(candidate).items()}
    replays=audit.get('semantic_replays')
    if not isinstance(replays,list) or len(replays)!=len(expected):raise ValueError('semantic replay evidence missing')
    seen=set()
    for replay in replays:
        day=replay.get('day')
        if (day in seen or day not in expected or replay.get('input_sha256')!=expected[day] or
            replay.get('exit_code')!=0 or
            re.fullmatch('[0-9a-f]{64}',replay.get('output_sha256','')) is None or
            re.fullmatch('[0-9a-f]{64}',replay.get('replay_binary_sha256','')) is None):
            raise ValueError('semantic replay does not bind candidate day bytes')
        seen.add(day)
    token=audit['original_sha256']
    backup=path.with_name(path.name+'.before-repair-'+token)
    if backup.is_symlink() or (backup.exists() and not stat.S_ISREG(backup.lstat().st_mode)):
        raise ValueError('immutable raw backup must be regular, never a symlink')
    if backup.exists():
        if backup.read_bytes()!=original:raise ValueError('immutable raw evidence differs')
    else:
        with backup.open('xb') as out:out.write(original);out.flush();os.fsync(out.fileno())
        backup.chmod(0o400)
    fsync_directory(path.parent)
    temporary=path.with_name(path.name+'.repair-'+str(os.getpid()))
    with temporary.open('xb') as out:out.write(candidate);out.flush();os.fsync(out.fileno())
    try:
        verify_quiescence()
        if path.is_symlink() or path.read_bytes()!=original:raise ValueError('raw changed at publication fence')
        os.replace(temporary,path);fsync_directory(path.parent)
    finally:
        if temporary.exists():temporary.unlink()
    return backup
