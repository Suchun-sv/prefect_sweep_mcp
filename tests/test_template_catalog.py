from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from template_catalog import TemplateCatalogError, load_template_catalog


class TemplateCatalogTests(unittest.TestCase):
    def _write_catalog(self, text: str) -> Path:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "catalog.yaml"
        path.write_text(textwrap.dedent(text))
        return path

    def test_load_valid_catalog(self):
        path = self._write_catalog(
            """
            templates:
              - name: practice_101
                description: Practice template
                deployment_name: practice_101
                repo_url: https://github.com/example/repo
                repo_local_path: ~/github/repo
                default_branch: main
                work_pool: CPU_pool
                work_queue: practice
                job_variables: {}
                default_cmd: echo hello
                allowed_launch_overrides: []
                allowed_tasks: [Practice]
            """
        )
        templates = load_template_catalog(path)
        self.assertEqual(len(templates), 1)
        self.assertEqual(templates[0].name, "practice_101")

    def test_duplicate_template_names_fail_validation(self):
        path = self._write_catalog(
            """
            templates:
              - name: duplicate
                description: One
                deployment_name: one
                repo_url: https://github.com/example/repo
                repo_local_path: ~/github/repo
                default_branch: main
                work_pool: CPU_pool
                work_queue: practice
                job_variables: {}
                default_cmd: echo one
              - name: duplicate
                description: Two
                deployment_name: two
                repo_url: https://github.com/example/repo2
                repo_local_path: ~/github/repo2
                default_branch: main
                work_pool: CPU_pool
                work_queue: practice
                job_variables: {}
                default_cmd: echo two
            """
        )
        with self.assertRaisesRegex(TemplateCatalogError, "Duplicate template name"):
            load_template_catalog(path)

    def test_malformed_yaml_fails_clearly(self):
        path = self._write_catalog("templates: [")
        with self.assertRaises(TemplateCatalogError):
            load_template_catalog(path)


if __name__ == "__main__":
    unittest.main()
