"""Independent data contract: fill a committed hole without rewriting known bytes."""
import hashlib
import json
import unittest
from deploy.market_data.ingestor.test_market_ingestor import event, market_ingestor

class MissingPrefixContract(unittest.TestCase):
    def fixture(self):
        rows=[];lines=[]
        for sequence in range(1,5):
            doc=json.loads(event(source_id='eastmoney-web-time-sales',source_sequence=sequence))
            doc['source']['source_instance_id']='8101d65c-bdba-4de3-83e0-8983506f159e'
            doc['event_id']=market_ingestor.canonical_event_identity(doc).hex()
            raw=market_ingestor.canonical_json(doc)
            row=dict(topic='gridedge/market/v1/XSHE/002256/trade',payload_hex=raw.hex(),
                     event_id=doc['event_id'],payload_sha256=hashlib.sha256(raw).hexdigest(),
                     source_sequence=sequence,qos=1)
            rows.append(row)
            lines.append(json.dumps(dict(topic=row['topic'],payload=list(raw))).encode()+b'\n')
        return rows,lines

    def test_missing_committed_middle_is_recovered_and_known_lines_preserved(self):
        from deploy.ops.market_raw_repair import prepare
        rows,lines=self.fixture(); original=lines[0]+lines[3]
        candidate,audit=prepare(original,rows,1787103010000000)
        self.assertTrue(candidate.startswith(lines[0]));self.assertTrue(candidate.endswith(lines[3]))
        self.assertEqual(audit['added_sequences'],[2,3])
        self.assertEqual(audit['original_sha256'],hashlib.sha256(original).hexdigest())

    def test_conflict_missing_commit_reordered_local_or_future_never_repairs(self):
        from deploy.ops.market_raw_repair import prepare
        rows,lines=self.fixture()
        for data,export,cutoff in [(lines[0]+lines[3],rows[:2]+rows[3:],1787103010000000),
            (lines[3]+lines[0],rows,1787103010000000),
            (lines[0]+lines[3],rows,1),
            (lines[0]+lines[3][:-1],rows,1787103010000000)]:
            with self.subTest(),self.assertRaises(ValueError):prepare(data,export,cutoff)
        bad=[dict(x) for x in rows];bad[1]['payload_sha256']='0'*64
        with self.assertRaises(ValueError):prepare(lines[0]+lines[3],bad,1787103010000000)

if __name__=='__main__':unittest.main()
