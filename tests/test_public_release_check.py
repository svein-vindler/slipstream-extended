from scripts.public_release_check import audit_paths, audit_public_metadata, audit_text


def test_public_release_paths_accept_templates_and_source_files():
    assert audit_paths(
        [
            ".env.local-bootstrap.example",
            "data/.gitkeep",
            "pipeline/granular.py",
            "worker/wrangler.jsonc",
        ]
    ) == []


def test_public_release_paths_reject_private_fitness_artifacts():
    violations = audit_paths(
        [
            ".dev.vars",
            ".granular/backfill/status.json",
            "data/activities.csv",
            "exports/activity_123.fit",
            "garmin_tokens.local.txt",
        ]
    )

    assert len(violations) == 5


def test_public_release_paths_reject_stale_windows_shell_installer():
    violations = audit_paths(["setup-windows.sh"])

    assert any("private local configuration" in item for item in violations)


def test_public_release_content_rejects_legacy_auth_instructions():
    violations = audit_text(
        "docs/INSTALL.md",
        "Authentication: No authentication\nwrangler secret put MCP_SECRET\n",
    )

    assert len(violations) == 2


def test_public_release_content_rejects_installation_specific_wrangler_values():
    violations = audit_text(
        "worker/wrangler.jsonc",
        '{"account_id":"0123456789abcdef",'
        '"vars":{"MCP_HOSTNAME":"fitness.example.workers.dev"}}',
    )

    assert len(violations) == 3


def test_public_release_content_allows_portable_wrangler_config():
    assert audit_text(
        "worker/wrangler.jsonc",
        '{"name":"slipstream-mcp","vars":{"REFRESH_COOLDOWN_MINUTES":"30"},'
        '"ratelimits":[{"namespace_id":"1001"}]}',
    ) == []


def test_public_metadata_accepts_safe_upstream_workflow(tmp_path):
    (tmp_path / ".github/workflows").mkdir(parents=True)
    (tmp_path / ".upstream-version").write_text("a" * 40, encoding="utf-8")
    (tmp_path / ".github/workflows/upstream-sync.yml").write_text(
        "issues: write\ngit ls-remote upstream refs/heads/main\n.upstream-version\n",
        encoding="utf-8",
    )
    for relative in (
        "CODE_OF_CONDUCT.md",
        "CONTRIBUTING.md",
        "LICENSE",
        "SECURITY.md",
        "docs/INSTALL.md",
        "docs/PUBLISHING.md",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("safe", encoding="utf-8")
    paths = [
        ".upstream-version",
        ".github/workflows/upstream-sync.yml",
        "CODE_OF_CONDUCT.md",
        "CONTRIBUTING.md",
        "LICENSE",
        "SECURITY.md",
        "docs/INSTALL.md",
        "docs/PUBLISHING.md",
    ]

    assert audit_public_metadata(tmp_path, paths) == []


def test_public_metadata_rejects_merge_and_private_repo_reference(tmp_path):
    (tmp_path / ".github/workflows").mkdir(parents=True)
    (tmp_path / ".github/workflows/upstream-sync.yml").write_text(
        "issues: write\ngit ls-remote upstream refs/heads/main\n"
        ".upstream-version\ngit merge upstream/main\n",
        encoding="utf-8",
    )
    (tmp_path / "README.md").write_text(
        "https://github.com/svein-vindler/slipstream/actions", encoding="utf-8"
    )

    violations = audit_public_metadata(
        tmp_path, [".github/workflows/upstream-sync.yml", "README.md"]
    )

    assert any("must not run git merge" in item for item in violations)
    assert any("old private repository reference" in item for item in violations)
