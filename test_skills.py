import json
from pathlib import Path
import tempfile
import unittest
import zipfile

from skills import SkillStore, load_bundle, metadata
from upload import Grant


class SkillTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        (self.source / 'SKILL.md').write_text('---\nname: explain-code\ndescription: Explain code clearly.\n---\nRead code and explain it.')
        (self.source / 'helper.py').write_text('print("helper")\n')
        self.grant = Grant(123, 'request-1', self.root / 'workspace', sender=8)
        self.grant.workspace.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def test_bundle_validation_and_zip(self):
        name, description, digest, files = load_bundle(self.source)
        self.assertEqual((name, description), ('explain-code', 'Explain code clearly.'))
        self.assertEqual(set(files), {'SKILL.md', 'helper.py'})
        archive = self.root / 'skill.zip'
        with zipfile.ZipFile(archive, 'w') as output:
            for path in self.source.rglob('*'):
                if path.is_file():
                    output.write(path, 'explain-code/' + path.relative_to(self.source).as_posix())
        self.assertEqual(load_bundle(archive)[2], digest)
        with self.assertRaises(ValueError):
            metadata(b'---\nname: bad_name\ndescription: no\n---\ntext')

    def test_store_revision_and_idempotency(self):
        store = SkillStore(self.root / 'skills')
        bundle = load_bundle(self.source)
        first = store.change(self.grant, {'op': 'install', 'path': str(self.source), 'key': 'install'}, bundle)
        self.assertEqual(first['status'], 'installed')
        patch = store.prepare(self.root / 'run')
        self.assertEqual(patch.suffix, '.yml')
        self.assertEqual((self.root / 'run/workspace/.group-skills/explain-code/SKILL.md').read_text(), (self.source / 'SKILL.md').read_text())
        repeated = store.change(self.grant, {'op': 'install', 'path': str(self.source), 'key': 'install'}, bundle)
        self.assertTrue(repeated['reused_result'])
        current = store.listing()[0]['revision']
        self.grant.message_id = 'request-2'
        with self.assertRaises(ValueError):
            store.change(self.grant, {'op': 'remove', 'name': 'explain-code', 'revision': 'stale', 'key': 'remove'}, None)
        removed = store.change(self.grant, {'op': 'remove', 'name': 'explain-code', 'revision': current, 'key': 'remove'}, None)
        self.assertEqual(removed['status'], 'removed')

    def test_zip_path_traversal_is_rejected(self):
        archive = self.root / 'bad.zip'
        with zipfile.ZipFile(archive, 'w') as output:
            output.writestr('../SKILL.md', '---\nname: bad\ndescription: bad\n---\nbody')
        with self.assertRaises(ValueError):
            load_bundle(archive)


if __name__ == '__main__':
    unittest.main()
