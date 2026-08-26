from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "templates"
STATIC = ROOT / "static"


def read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8-sig")


class FrontendAssetTests(unittest.TestCase):
    def test_global_head_does_not_load_datatables_css(self):
        head = read("templates/includes/head.html")

        self.assertNotIn("cdn.datatables.net", head)
        self.assertNotIn("buttons.dataTables.min.css", head)
        self.assertIn("static', filename='style.css'", head)
        self.assertIn("page_css_files", head)

    def test_global_footer_is_minimal(self):
        footer = read("templates/includes/footer.html")

        for forbidden in (
            "code.jquery.com",
            "jquery.dataTables.min.js",
            "dataTables.buttons.min.js",
            "buttons.html5.min.js",
            "jszip.min.js",
            "usuarios_table.js",
        ):
            self.assertNotIn(forbidden, footer)

        self.assertIn("js/flash_messages.js", footer)
        self.assertIn("js/status_online.js", footer)
        self.assertIn("session.usuario_logado", footer)
        self.assertIn("js/logs_table.js", footer)

    def test_no_active_template_uses_datatables_or_legacy_users_table(self):
        template_text = "\n".join(
            path.read_text(encoding="utf-8-sig")
            for path in TEMPLATES.rglob("*.html")
        )

        for forbidden in (
            "DataTable(",
            ".DataTable(",
            "jQuery(",
            "jquery.dataTables",
            "dataTables.buttons",
            "buttons.html5",
            "jszip.min.js",
            "js/usuarios_table.js",
            'id="usuariosTable"',
            'id="nameSearch"',
            'class="fd-btn',
        ):
            self.assertNotIn(forbidden, template_text)

    def test_dashboard_keeps_only_its_essential_page_script(self):
        dashboard = read("templates/dashboard_operacional.html")

        self.assertIn("js/dashboard_operacional.js", dashboard)
        self.assertIn("<script defer", dashboard)
        self.assertNotIn("jquery", dashboard.lower())
        self.assertNotIn("datatable", dashboard.lower())
        self.assertNotIn("jszip", dashboard.lower())

    def test_representative_pages_keep_page_specific_assets(self):
        representative = {
            "templates/relatorios_ofs_os.html": (
                "css/relatorios.css",
                "js/relatorios_ofs_os.js",
            ),
            "templates/bi_activities.html": (
                "css/bi_activities.css",
                "js/bi_activities.js",
            ),
            "templates/ofs_activity_type_config/ofs_activity_type_config.html": (
                "css/ofs_config_shared.css",
                "ofs_activity_type_config/ofs_activity_type_config.js",
            ),
        }

        for template_path, expected_assets in representative.items():
            with self.subTest(template=template_path):
                source = read(template_path)
                for asset in expected_assets:
                    self.assertIn(asset, source)

    def test_only_legacy_unreferenced_script_depends_on_jquery_datatables(self):
        matches = []
        for path in STATIC.rglob("*.js"):
            source = path.read_text(encoding="utf-8-sig")
            if "DataTable(" in source or "jQuery(" in source or "$(" in source:
                matches.append(path.relative_to(ROOT).as_posix())

        self.assertEqual(matches, ["static/js/usuarios_table.js"])


if __name__ == "__main__":
    unittest.main()
