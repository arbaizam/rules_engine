from tools import build_release_wheel


def test_release_builder_removes_stale_outputs_and_builds_one_versioned_wheel(
    tmp_path,
    monkeypatch,
):
    """Release builds cannot retain stale modules or multiple wheel identities."""
    build_dir = tmp_path / "build"
    dist_dir = tmp_path / "dist"
    egg_info_dir = tmp_path / "rules_engine.egg-info"
    build_dir.mkdir()
    egg_info_dir.mkdir()
    dist_dir.mkdir()
    (build_dir / "stale_module.py").write_text("stale", encoding="utf-8")
    (dist_dir / "rules_engine-0.1.0-py3-none-any.whl").write_bytes(b"old")
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "rules-engine"\nversion = "0.2.0"\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(build_release_wheel, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(build_release_wheel, "BUILD_DIR", build_dir)
    monkeypatch.setattr(build_release_wheel, "DIST_DIR", dist_dir)
    monkeypatch.setattr(build_release_wheel, "EGG_INFO_DIR", egg_info_dir)

    def fake_build(command, *, cwd, check):
        assert command == [build_release_wheel.sys.executable, "-m", "build", "--wheel"]
        assert cwd == tmp_path
        assert check is True
        (dist_dir / "rules_engine-0.2.0-py3-none-any.whl").write_bytes(b"new")

    monkeypatch.setattr(build_release_wheel.subprocess, "run", fake_build)

    build_release_wheel.main()

    assert not build_dir.exists()
    assert not egg_info_dir.exists()
    assert [path.name for path in dist_dir.glob("*.whl")] == [
        "rules_engine-0.2.0-py3-none-any.whl"
    ]


def test_release_builder_refuses_directory_outside_repository(tmp_path, monkeypatch):
    """Cleanup guards reject a directory whose parent is not the repository root."""
    monkeypatch.setattr(build_release_wheel, "REPO_ROOT", tmp_path / "repo")
    outside = tmp_path / "outside" / "build"

    try:
        build_release_wheel._remove_build_directory(outside)
    except RuntimeError as exc:
        assert "outside repository" in str(exc)
    else:
        raise AssertionError("Expected cleanup guard to reject an outside directory.")
