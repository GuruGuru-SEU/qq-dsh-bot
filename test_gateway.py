import unittest
from gateway import Gateway


class GatewayTests(unittest.TestCase):
    def test_snowluma_history_walks_back_with_signed_anchor(self):
        gateway = Gateway('snowluma')
        page = gateway.response('get_group_msg_history', {'messages': [
            {'message_id': -42, 'message': [{'type': 'reply', 'data': {'id': 9}}]}]})
        anchor = page['messages'][0]['message_id']
        self.assertEqual(anchor, 'snowluma:-42')
        self.assertEqual(page['messages'][0]['message'][0]['data']['id'], 'snowluma:9')
        params = gateway.params('get_group_msg_history', gateway.history_params(123, anchor))
        self.assertEqual(params, {'group_id': 123, 'count': 100, 'reverse_order': True, 'message_id': -42})
        self.assertNotIn('message_id', gateway.history_params(123))

    def test_old_archive_ids_never_reach_other_gateway(self):
        for gateway, old in [(Gateway('snowluma'), '42'), (Gateway('napcat'), 'snowluma:42')]:
            for action in ['get_msg', 'fetch_ptt_text', 'set_msg_emoji_like']:
                with self.assertRaises(ValueError):
                    gateway.params(action, {'message_id': old})
            result = gateway.params('send_group_msg', {'message': [
                {'type': 'reply', 'data': {'id': old}}, {'type': 'text', 'data': {'text': 'done'}}]})
            self.assertEqual(result['message'], [{'type': 'text', 'data': {'text': 'done'}}])

    def test_napcat_cursor_unchanged(self):
        gateway = Gateway()
        self.assertEqual(gateway.history_params(123, '42')['message_seq'], '42')
        self.assertFalse(gateway.history_params(123)['reverse_order'])

    def test_send_and_lookup_roundtrip(self):
        gateway = Gateway('snowluma')
        result = gateway.response('send_group_msg', {'message_id': -7})
        self.assertEqual(gateway.params('get_msg', result), {'message_id': -7})
        request = {'message': [{'type': 'reply', 'data': {'id': 'snowluma:-7'}}]}
        self.assertEqual(gateway.params('send_group_msg', request)['message'][0]['data']['id'], '-7')
        self.assertEqual(request['message'][0]['data']['id'], 'snowluma:-7')

    def test_both_backends_can_share_archive(self):
        import tempfile
        from pathlib import Path
        from history import History
        with tempfile.TemporaryDirectory() as tmp:
            archive = History(Path(tmp) / 'history.sqlite3')
            old = {'message_id': '42', 'message': [{'type': 'text', 'data': {'text': 'old'}}]}
            new = Gateway('snowluma').message({**old, 'message': [{'type': 'text', 'data': {'text': 'new'}}]})
            archive.save(old)
            archive.save(new)
            self.assertEqual(archive.message_text('42'), 'old')
            self.assertEqual(archive.message_text('snowluma:42'), 'new')
