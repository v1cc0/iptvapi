use std::process::Command;

fn main() {
    let commit = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=GIT_COMMIT_HASH={}", commit);
    // Tell Cargo to re-run this build script if HEAD changes
    println!("cargo:rerun-if-changed=.git/HEAD");
    if std::path::Path::new(".git/refs/heads").exists() {
        println!("cargo:rerun-if-changed=.git/refs/heads");
    }
}
