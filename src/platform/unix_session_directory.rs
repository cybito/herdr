//! Private session directories, created without following replaceable symlinks.

use std::fs::{File, Permissions};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

pub(crate) fn prepare_session_directory(path: &Path) -> io::Result<()> {
    let bytes = path.as_os_str().as_bytes();
    if !path.is_absolute()
        || bytes.contains(&0)
        || bytes
            .split(|byte| *byte == b'/')
            .skip(1)
            .any(|part| part.is_empty() || part == b"." || part == b"..")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "session directory must be an absolute, normalized path",
        ));
    }

    // SAFETY: geteuid has no pointer arguments or ownership requirements.
    let uid = unsafe { libc::geteuid() };
    let directory = open_directories(File::open("/")?, path, uid, &mut 40, true, false)?;

    let metadata = directory.metadata()?;
    if metadata.uid() != uid
        || metadata.mode() & 0o022 != 0
        || metadata.mode() & 0o700 != 0o700
        || metadata.mode() & 0o7000 != 0
    {
        return Err(unsafe_directory());
    }
    // Earlier Herdr versions created owned 0755 session directories. Restrict
    // only this session directory, through its verified fd, without changing
    // ancestor modes, stored state, or already-private directory metadata.
    if metadata.mode() & 0o7777 != 0o700 {
        directory.set_permissions(Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn trusted_ancestor(metadata: &std::fs::Metadata, uid: u32) -> io::Result<()> {
    // Root-owned sticky temporary bases protect owned child entries.
    let protected_temporary_base = metadata.uid() == 0 && metadata.mode() & 0o1000 != 0;
    if (metadata.uid() != uid && metadata.uid() != 0)
        || (metadata.mode() & 0o022 != 0 && !protected_temporary_base)
    {
        return Err(unsafe_directory());
    }
    Ok(())
}

// Ancestors need traversal, not directory-listing access. The session leaf
// still uses a normal descriptor because it must support verified fd chmod.
#[cfg(any(target_os = "linux", target_os = "android"))]
const ANCESTOR_ACCESS: libc::c_int = libc::O_PATH;
#[cfg(target_vendor = "apple")]
const ANCESTOR_ACCESS: libc::c_int = libc::O_SEARCH;
#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
const ANCESTOR_ACCESS: libc::c_int = libc::O_RDONLY;

fn open_directories(
    mut directory: File,
    path: &Path,
    uid: u32,
    remaining_links: &mut u8,
    create: bool,
    allow_system_leaf_alias: bool,
) -> io::Result<File> {
    let mut components = path
        .components()
        .filter(|component| *component != std::path::Component::RootDir)
        .peekable();
    let mut name = Vec::with_capacity(path.as_os_str().as_bytes().len() + 1);
    while let Some(component) = components.next() {
        let metadata = directory.metadata()?;
        trusted_ancestor(&metadata, uid)?;
        name.clear();
        name.extend_from_slice(component.as_os_str().as_bytes());
        name.push(0);
        let access = if components.peek().is_none() && !allow_system_leaf_alias {
            libc::O_RDONLY
        } else {
            ANCESTOR_ACCESS
        };
        let flags = access | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
        // SAFETY: directory and the terminated component remain live through openat.
        let mut fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr().cast(), flags) };
        if fd < 0 {
            let error = io::Error::last_os_error();
            // Only immutable root-owned ancestor aliases may be resolved. In
            // particular, macOS /tmp and /var are aliases, not unsafe user links.
            let mut entry = std::mem::MaybeUninit::<libc::stat>::uninit();
            let system_alias = (components.peek().is_some() || allow_system_leaf_alias)
                && metadata.uid() == 0
                && metadata.mode() & 0o022 == 0
                // SAFETY: the live directory, terminated name, and writable stat
                // storage satisfy fstatat; successful output is initialized.
                && unsafe {
                    libc::fstatat(
                        directory.as_raw_fd(),
                        name.as_ptr().cast(),
                        entry.as_mut_ptr(),
                        libc::AT_SYMLINK_NOFOLLOW,
                    )
                } == 0;
            if system_alias {
                // SAFETY: system_alias implies successful fstatat above.
                let entry = unsafe { entry.assume_init() };
                if entry.st_uid == 0 && entry.st_mode & libc::S_IFMT == libc::S_IFLNK {
                    if *remaining_links == 0 {
                        return Err(io::Error::from_raw_os_error(libc::ELOOP));
                    }
                    *remaining_links -= 1;
                    let mut target = [0_u8; libc::PATH_MAX as usize];
                    // SAFETY: directory/name are live and target is writable
                    // for the full length supplied to readlinkat.
                    let length = unsafe {
                        libc::readlinkat(
                            directory.as_raw_fd(),
                            name.as_ptr().cast(),
                            target.as_mut_ptr().cast(),
                            target.len(),
                        )
                    };
                    if length < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    if length as usize == target.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "system directory alias target exceeds path limit",
                        ));
                    }
                    let target = Path::new(std::ffi::OsStr::from_bytes(&target[..length as usize]));
                    let base = if target.is_absolute() {
                        File::open("/")?
                    } else {
                        directory
                    };
                    // Walk every target component with the same trust checks;
                    // never create a missing system alias target.
                    directory = open_directories(base, target, uid, remaining_links, false, true)?;
                    continue;
                }
            }
            if !create || error.kind() != io::ErrorKind::NotFound {
                return Err(error);
            }
            // SAFETY: the validated parent fd and terminated component remain live.
            if unsafe { libc::mkdirat(directory.as_raw_fd(), name.as_ptr().cast(), 0o700) } < 0 {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::AlreadyExists {
                    return Err(error);
                }
            }
            // Accept a concurrent creator only after no-follow open and ownership checks.
            // SAFETY: directory and the terminated component remain live through openat.
            fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr().cast(), flags) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        // SAFETY: fd is a newly opened, valid descriptor; File takes sole ownership.
        directory = unsafe { File::from_raw_fd(fd) };
    }
    trusted_ancestor(&directory.metadata()?, uid)?;
    Ok(directory)
}

fn unsafe_directory() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "session directory must be user-owned, writable by its owner only, and have safe ancestors",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, DirBuilder};
    use std::os::unix::fs::{symlink, DirBuilderExt};
    use std::path::PathBuf;

    struct Fixture(PathBuf);

    impl Fixture {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir()
                .canonicalize()
                .unwrap()
                .join(format!("herdr-session-{name}-{}", std::process::id()));
            DirBuilder::new().mode(0o700).create(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn legacy_session_directory_preserves_state_and_ancestor_modes() {
        let fixture = Fixture::new("legacy");
        let parent = fixture.0.join("sessions");
        let directory = parent.join("work");
        fs::create_dir_all(&directory).unwrap();
        fs::set_permissions(&parent, Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&directory, Permissions::from_mode(0o755)).unwrap();
        fs::write(directory.join("session.json"), b"preserved session state").unwrap();
        let old_inode = fs::metadata(&directory).unwrap().ino();

        prepare_session_directory(&directory).unwrap();
        let private = fs::metadata(&directory).unwrap();
        assert_eq!(private.mode() & 0o7777, 0o700);
        assert_eq!(private.ino(), old_inode);
        assert_eq!(fs::metadata(&parent).unwrap().mode() & 0o7777, 0o755);
        assert_eq!(
            fs::read(directory.join("session.json")).unwrap(),
            b"preserved session state"
        );

        prepare_session_directory(&directory).unwrap();
        let repeated = fs::metadata(&directory).unwrap();
        assert_eq!(
            (repeated.ctime(), repeated.ctime_nsec()),
            (private.ctime(), private.ctime_nsec())
        );
        assert_eq!(repeated.ino(), old_inode);
    }

    #[test]
    fn linked_session_directory_components_do_not_mutate_external_targets() {
        let fixture = Fixture::new("links");
        let outside = fixture.0.join("outside");
        DirBuilder::new().mode(0o755).create(&outside).unwrap();
        fs::set_permissions(&outside, Permissions::from_mode(0o755)).unwrap();
        fs::write(outside.join("state"), b"external state").unwrap();
        let outside_inode = fs::metadata(&outside).unwrap().ino();
        let link = fixture.0.join("linked");
        symlink(&outside, &link).unwrap();

        for target in [&link, &link.join("work")] {
            assert!(prepare_session_directory(target).is_err());
            assert_eq!(fs::read(outside.join("state")).unwrap(), b"external state");
            let unchanged = fs::metadata(&outside).unwrap();
            assert_eq!(unchanged.ino(), outside_inode);
            assert_eq!(unchanged.mode() & 0o7777, 0o755);
            assert!(!outside.join("work").exists());
            assert!(fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink());
        }
    }

    #[test]
    fn unsafe_or_read_only_session_directories_are_not_adopted() {
        let fixture = Fixture::new("unsafe");
        for mode in [0o775, 0o500, 0o1700] {
            let directory = fixture.0.join(format!("mode-{mode:o}"));
            DirBuilder::new().mode(0o700).create(&directory).unwrap();
            fs::write(directory.join("state"), b"retained state").unwrap();
            fs::set_permissions(&directory, Permissions::from_mode(mode)).unwrap();
            let old_inode = fs::metadata(&directory).unwrap().ino();

            let error = prepare_session_directory(&directory).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
            assert_eq!(fs::metadata(&directory).unwrap().mode() & 0o7777, mode);
            assert_eq!(fs::metadata(&directory).unwrap().ino(), old_inode);
            assert_eq!(
                fs::read(directory.join("state")).unwrap(),
                b"retained state"
            );
            fs::set_permissions(&directory, Permissions::from_mode(0o700)).unwrap();
        }

        let parent = fixture.0.join("writable-parent");
        DirBuilder::new().mode(0o700).create(&parent).unwrap();
        fs::set_permissions(&parent, Permissions::from_mode(0o777)).unwrap();
        let error = prepare_session_directory(&parent.join("work")).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(!parent.join("work").exists());
        assert_eq!(fs::metadata(parent).unwrap().mode() & 0o7777, 0o777);
    }

    #[test]
    fn system_temporary_ancestor_spelling_preserves_private_session_boundary() {
        let path = PathBuf::from("/tmp")
            .join(format!("herdr-session-system-alias-{}", std::process::id()));
        DirBuilder::new().mode(0o700).create(&path).unwrap();
        let fixture = Fixture(path);
        let base_before = fs::metadata("/tmp").unwrap();
        fs::write(fixture.0.join("state"), b"retained under system alias").unwrap();

        let session = fixture.0.join("sessions/work");
        prepare_session_directory(&session).unwrap();

        let session_metadata = fs::metadata(&session).unwrap();
        assert_eq!(
            session_metadata.uid(),
            fs::metadata(&fixture.0).unwrap().uid()
        );
        assert_eq!(session_metadata.mode() & 0o7777, 0o700);
        assert_eq!(
            fs::read(fixture.0.join("state")).unwrap(),
            b"retained under system alias"
        );
        let base_after = fs::metadata("/tmp").unwrap();
        assert_eq!(base_after.ino(), base_before.ino());
        assert_eq!(base_after.mode(), base_before.mode());
    }

    #[test]
    fn search_only_ancestor_does_not_require_listing_or_change_permissions() {
        let fixture = Fixture::new("search-only");
        let parent = fixture.0.join("traversable");
        let session = parent.join("work");
        fs::create_dir(&parent).unwrap();
        DirBuilder::new().mode(0o755).create(&session).unwrap();
        fs::write(session.join("state"), b"retained state").unwrap();
        fs::set_permissions(&parent, Permissions::from_mode(0o100)).unwrap();

        let result = prepare_session_directory(&session);
        let parent_mode = fs::metadata(&parent).unwrap().mode() & 0o7777;
        fs::set_permissions(&parent, Permissions::from_mode(0o700)).unwrap();
        result.unwrap();

        assert_eq!(parent_mode, 0o100);
        assert_eq!(fs::metadata(&session).unwrap().mode() & 0o7777, 0o700);
        assert_eq!(fs::read(session.join("state")).unwrap(), b"retained state");
    }

    #[test]
    fn invalid_paths_are_refused_before_creating_any_component() {
        let fixture = Fixture::new("invalid-path");
        fs::write(fixture.0.join("state"), b"retained state").unwrap();
        for path in [
            PathBuf::from("relative-session/child"),
            PathBuf::from("/"),
            fixture.0.join("not-created/../escaped"),
            fixture.0.join("not-created/./escaped"),
            fixture
                .0
                .join(std::ffi::OsStr::from_bytes(b"not-created/\0/escaped")),
        ] {
            let error = prepare_session_directory(&path).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(!fixture.0.join("not-created").exists());
            assert!(!fixture.0.join("escaped").exists());
            assert_eq!(
                fs::read(fixture.0.join("state")).unwrap(),
                b"retained state"
            );
        }
    }
}
