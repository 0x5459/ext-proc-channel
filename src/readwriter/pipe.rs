use std::{
    ffi::{OsStr, OsString},
    io::{self, Result},
    iter,
    path::Path,
    pin::Pin,
    process::{self, Stdio},
    slice,
    task::{Context, Poll},
};

use futures::future::{ready, Ready};
use pin_project::pin_project;
use tokio::{
    io::{stdin, stdout, AsyncRead, AsyncWrite, ReadBuf, Stdin, Stdout},
    process::{ChildStdin, ChildStdout, Command},
};

use super::reconnect::Reconnectable;

#[pin_project]
pub struct Pipe<R, W> {
    id: u32,
    #[pin]
    reader: R,
    #[pin]
    writer: W,
}

/// Executes the command `bin` as a child process and establish pipe connection,
///
/// # Example
/// ```
/// use std::{error::Error, ffi::OsString, iter::empty, path::PathBuf};
/// use tokio::io::{AsyncReadExt, AsyncWriteExt};
///
/// use ext_proc_channel::readwriter::{pipe::connect};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn Error>> {
///     let mut cat = connect(PathBuf::from("/bin/cat"), empty::<OsString>())?;
///     // Write some data.
///     cat.write_all(b"hello world").await?;
///
///     let mut buf = vec![0; 11];
///     cat.read_exact(&mut buf).await?;
///     assert_eq!(b"hello world", buf.as_slice());
///     Ok(())
/// }
/// ```
pub fn connect(
    bin: impl AsRef<Path>,
    args: impl IntoIterator<Item = impl AsRef<OsStr>>,
) -> io::Result<Pipe<ChildStdout, ChildStdin>> {
    let cmd = Command::new(bin.as_ref())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .args(args)
        .spawn()?;

    match (cmd.id(), cmd.stdout, cmd.stdin) {
        (Some(id), Some(reader), Some(writer)) => Ok(Pipe { id, reader, writer }),
        _ => Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "child process exited unexpectedly",
        )),
    }
}

/// Listen in standard I/O
pub fn listen() -> Pipe<Stdin, Stdout> {
    Pipe {
        id: process::id(),
        reader: stdin(),
        writer: stdout(),
    }
}

pub trait IntoPipeConnectArgs {
    type Path: AsRef<Path>;

    type Arg: AsRef<OsStr>;

    type ArgsIt: IntoIterator<Item = Self::Arg>;

    fn bin_path(&self) -> Self::Path;
    fn args(&self) -> Self::ArgsIt;
}

impl<'a> IntoPipeConnectArgs for &'a Path {
    type Path = &'a Path;

    type Arg = &'a OsString;

    type ArgsIt = iter::Empty<&'a OsString>;

    fn bin_path(&self) -> Self::Path {
        self
    }

    fn args(&self) -> Self::ArgsIt {
        iter::empty()
    }
}

impl<'a> IntoPipeConnectArgs for (&'a Path, &'a [OsString]) {
    type Path = &'a Path;

    // TODO(0x5459): We need GAT to replace OsString with OsStr
    type Arg = &'a OsString;

    type ArgsIt = slice::Iter<'a, OsString>;

    fn bin_path(&self) -> Self::Path {
        self.0
    }

    fn args(&self) -> Self::ArgsIt {
        self.1.iter()
    }
}

impl<C> Reconnectable<C> for Pipe<ChildStdout, ChildStdin>
where
    C: IntoPipeConnectArgs,
{
    type ConnectingFut = Ready<io::Result<Self>>;

    fn connect(ctor_arg: C) -> Self::ConnectingFut {
        ready(connect(ctor_arg.bin_path(), ctor_arg.args()))
    }
}

impl<R, W> AsyncRead for Pipe<R, W>
where
    R: AsyncRead,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        self.project().reader.poll_read(cx, buf)
    }
}

impl<R, W> AsyncWrite for Pipe<R, W>
where
    W: AsyncWrite,
{
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        self.project().writer.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().writer.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().writer.poll_shutdown(cx)
    }
}
