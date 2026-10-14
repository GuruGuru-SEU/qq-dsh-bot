"""Translate gateway-specific cursors and keep message IDs separate across backends."""
from copy import deepcopy


class Gateway:
    def __init__(self, backend='napcat'):
        if backend not in {'napcat', 'snowluma'}:
            raise ValueError('gateway must be napcat or snowluma')
        self.backend = backend

    def local_id(self, value):
        value = str(value)
        return 'snowluma:' + value if self.backend == 'snowluma' else value

    def wire_id(self, value):
        value = str(value)
        if self.backend == 'snowluma':
            if not value.startswith('snowluma:'):
                raise ValueError('Message belongs to another gateway')
            value = value.removeprefix('snowluma:')
        if not value.lstrip('-').isdigit():
            raise ValueError('Not a gateway message ID')
        return int(value)

    def message(self, value):
        value = deepcopy(value)
        if not isinstance(value, dict):
            return value
        if value.get('message_id') is not None:
            value['message_id'] = self.local_id(value['message_id'])
        if isinstance(value.get('message'), list):
            for segment in value['message']:
                if segment.get('type') == 'reply' and 'id' in segment.get('data', {}):
                    segment['data']['id'] = self.local_id(segment['data']['id'])
        return value

    def response(self, action, value):
        if self.backend == 'napcat':
            return value
        if action == 'get_group_msg_history' and isinstance(value, dict):
            return {**value, 'messages': [self.message(m) for m in value.get('messages', [])]}
        if action in {'get_msg', 'send_group_msg'}:
            return self.message(value)
        return value

    def params(self, action, params):
        params = deepcopy(params)
        if self.backend == 'napcat':
            if str(params.get('message_id', '')).startswith('snowluma:'):
                raise ValueError('Message belongs to another gateway')
        elif 'message_id' in params:
            params['message_id'] = self.wire_id(params['message_id'])
        if action == 'send_group_msg':
            segments = []
            for segment in params.get('message', []):
                if segment.get('type') == 'reply':
                    try:
                        identity = self.wire_id(segment['data']['id'])
                    except ValueError:
                        # A resumed job may refer to a message from the old gateway.
                        # Send its answer without an invalid/cross-backend quote.
                        continue
                    if self.backend == 'snowluma':
                        segment['data']['id'] = str(identity)
                segments.append(segment)
            params['message'] = segments
        return params

    def history_params(self, group, cursor=None):
        if self.backend == 'snowluma':
            params = {'group_id': group, 'count': 100, 'reverse_order': True}
            if cursor is not None:
                params['message_id'] = cursor
            return params
        params = {'group_id': group, 'count': 100, 'reverse_order': False,
                  'parse_mult_msg': False, 'disable_get_url': True}
        if cursor is not None:
            params['message_seq'] = cursor
        return params
