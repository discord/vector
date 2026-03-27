use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::{fs, fs::remove_file, path::Path};

use crate::internal_events::UnixSocketFileDeleteError;

pub const UNNAMED_SOCKET_HOST: &str = "(unnamed)";

/// Remove a stale Unix domain socket file left over from a previous run (e.g.
/// after a crash or SIGKILL). If the path exists but is not a socket, we leave
/// it alone and log an error — we should never silently delete a regular file
/// that someone may have placed at this path by mistake.
pub fn remove_stale_socket(path: &Path) {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if !meta.file_type().is_socket() {
                error!(
                    message = "Socket path already exists and is not a UNIX socket. Remove the file or choose a different path.",
                    path = ?path,
                );
                return;
            }
            match remove_file(path) {
                Ok(()) => {
                    info!(message = "Removed stale UNIX socket file.", path = ?path);
                }
                Err(error) => {
                    emit!(UnixSocketFileDeleteError { path, error });
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            warn!(message = "Unable to check socket path.", path = ?path, %error);
        }
    }
}

pub fn change_socket_permissions(path: &Path, perms: Option<u32>) -> crate::Result<()> {
    if let Some(mode) = perms {
        match fs::set_permissions(path, fs::Permissions::from_mode(mode)) {
            Ok(_) => debug!(message = "Socket permissions updated.", permission = mode),
            Err(e) => {
                if let Err(error) = remove_file(path) {
                    emit!(UnixSocketFileDeleteError { path, error });
                }
                return Err(Box::new(e));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;
    use tempfile::tempdir;

    #[test]
    fn remove_stale_socket_removes_existing_socket() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sock");

        drop(UnixListener::bind(&path).unwrap());
        assert!(path.exists());

        remove_stale_socket(&path);
        assert!(!path.exists(), "stale socket file should have been removed");
    }

    #[test]
    fn remove_stale_socket_does_not_delete_regular_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("not_a_socket.txt");

        fs::write(&path, "important data").unwrap();
        assert!(path.exists());

        remove_stale_socket(&path);
        assert!(path.exists(), "regular file must not be deleted");
        assert_eq!(fs::read_to_string(&path).unwrap(), "important data");
    }

    #[test]
    fn remove_stale_socket_noop_when_path_missing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.sock");

        remove_stale_socket(&path);
        assert!(!path.exists());
    }
}
