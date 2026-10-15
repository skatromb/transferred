//! Owns a disposable database container and its published port.

use std::path::Path;
use std::process::Command;

pub(crate) struct Container {
    identifier: String,
    port: u16,
}

impl Container {
    #[expect(
        clippy::too_many_lines,
        reason = "establishes cleanup ownership before resolving the published port"
    )]
    pub(crate) fn start(port: u16, arguments: &[&str]) -> Self {
        let output = Command::new("docker")
            .args(["run", "-d", "--rm", "-p", &format!("127.0.0.1::{port}")])
            .args(arguments)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut container = Self {
            identifier: String::from_utf8(output.stdout).unwrap().trim().to_owned(),
            port: 0,
        };
        let mapped = Command::new("docker")
            .args(["port", &container.identifier, &format!("{port}/tcp")])
            .output()
            .unwrap();
        assert!(
            mapped.status.success(),
            "{}",
            String::from_utf8_lossy(&mapped.stderr)
        );
        container.port = String::from_utf8(mapped.stdout)
            .unwrap()
            .trim()
            .rsplit(':')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        container
    }

    pub(crate) fn logs(&self) -> String {
        let output = Command::new("docker")
            .args(["logs", &self.identifier])
            .output()
            .unwrap();
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    pub(crate) const fn port(&self) -> u16 {
        self.port
    }

    pub(crate) fn copy_from(&self, source: &str, destination: &Path) {
        let output = Command::new("docker")
            .args(["cp", &format!("{}:{source}", self.identifier)])
            .arg(destination)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        drop(
            Command::new("docker")
                .args(["rm", "--force", "--volumes", &self.identifier])
                .output(),
        );
    }
}
