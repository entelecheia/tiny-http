use std::io::Read;
use std::io::Result as IoResult;
use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver, Sender};

/// A `Reader` that reads exactly the number of bytes from a sub-reader.
///
/// If the limit is reached, it returns EOF. If the limit is not reached
/// when the destructor is called, the remaining bytes will be read and
/// thrown away.
pub struct EqualReader<R>
where
    R: Read,
{
    reader: R,
    size: usize,
    last_read_signal: Sender<IoResult<()>>,
}

impl<R> EqualReader<R>
where
    R: Read,
{
    pub fn new(reader: R, size: usize) -> (EqualReader<R>, Receiver<IoResult<()>>) {
        let (tx, rx) = channel();

        let r = EqualReader {
            reader,
            size,
            last_read_signal: tx,
        };

        (r, rx)
    }
}

impl<R> Read for EqualReader<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if self.size == 0 {
            return Ok(0);
        }

        let buf = if buf.len() < self.size {
            buf
        } else {
            &mut buf[..self.size]
        };

        match self.reader.read(buf) {
            Ok(len) => {
                self.size -= len;
                Ok(len)
            }
            err @ Err(_) => err,
        }
    }
}

impl<R> Drop for EqualReader<R>
where
    R: Read,
{
    fn drop(&mut self) {
        let mut remaining_to_read = self.size;

        // A fixed-size buffer bounds memory regardless of the declared length.
        let mut buf = vec![0u8; 8192];

        while remaining_to_read > 0 {
            // Never ask for more than remains; preserves the exact drain semantics.
            let chunk = buf.len().min(remaining_to_read);

            match self.reader.read(&mut buf[..chunk]) {
                Err(e) => {
                    self.last_read_signal.send(Err(e)).ok();
                    break;
                }
                Ok(0) => {
                    self.last_read_signal.send(Ok(())).ok();
                    break;
                }
                Ok(other) => {
                    remaining_to_read -= other;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EqualReader;
    use std::cell::Cell;
    use std::io::Cursor;
    use std::io::Read;
    use std::io::Result as IoResult;
    use std::rc::Rc;

    #[test]
    fn test_limit() {
        use std::io::Cursor;

        let mut org_reader = Cursor::new("hello world".to_string().into_bytes());

        {
            let (mut equal_reader, _) = EqualReader::new(org_reader.by_ref(), 5);

            let mut string = String::new();
            equal_reader.read_to_string(&mut string).unwrap();
            assert_eq!(string, "hello");
        }

        let mut string = String::new();
        org_reader.read_to_string(&mut string).unwrap();
        assert_eq!(string, " world");
    }

    #[test]
    fn test_not_enough() {
        use std::io::Cursor;

        let mut org_reader = Cursor::new("hello world".to_string().into_bytes());

        {
            let (mut equal_reader, _) = EqualReader::new(org_reader.by_ref(), 5);

            let mut vec = [0];
            equal_reader.read_exact(&mut vec).unwrap();
            assert_eq!(vec[0], b'h');
        }

        let mut string = String::new();
        org_reader.read_to_string(&mut string).unwrap();
        assert_eq!(string, " world");
    }

    /// A reader whose source is much shorter than the declared size, recording
    /// how many bytes each `read` call is asked for.
    struct ShortSource {
        inner: Cursor<Vec<u8>>,
        remaining: usize,
        max_requested: Rc<Cell<usize>>,
    }

    impl Read for ShortSource {
        fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
            self.max_requested
                .set(::std::cmp::max(self.max_requested.get(), buf.len()));
            let n = ::std::cmp::min(buf.len(), self.remaining);
            let read = self.inner.read(&mut buf[..n])?;
            self.remaining -= read;
            Ok(read)
        }
    }

    #[test]
    fn test_drop_drain_requests_at_most_the_buffer_size() {
        use std::cell::Cell;
        use std::io::Cursor;
        use std::rc::Rc;

        let max_requested = Rc::new(Cell::new(0usize));

        let (equal_reader, rx) = EqualReader::new(
            ShortSource {
                inner: Cursor::new(vec![b'x'; 100]),
                remaining: 100,
                max_requested: Rc::clone(&max_requested),
            },
            64 * 1024,
        );

        drop(equal_reader);

        assert!(rx.recv().unwrap().is_ok(), "EOF drain must signal Ok(())");
        assert!(
            max_requested.get() <= 8192,
            "drop must not ask for more than 8192 bytes per read, got {}",
            max_requested.get()
        );
    }

    #[test]
    fn test_drop_forwards_read_errors_on_the_signal() {
        struct FailingReader;

        impl Read for FailingReader {
            fn read(&mut self, _buf: &mut [u8]) -> IoResult<usize> {
                Err(::std::io::Error::new(
                    ::std::io::ErrorKind::Other,
                    "boom",
                ))
            }
        }

        let (mut equal_reader, rx) = EqualReader::new(FailingReader, 42);

        let mut buf = [0u8; 8];
        let err = equal_reader.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), ::std::io::ErrorKind::Other);

        drop(equal_reader);

        let signaled = rx.recv().unwrap().unwrap_err();
        assert_eq!(signaled.kind(), ::std::io::ErrorKind::Other);
    }

    #[test]
    fn test_drop_full_drain_sends_nothing() {
        use std::io::Cursor;

        let mut org_reader = Cursor::new(vec![b'y'; 32]);

        let (mut equal_reader, rx) = EqualReader::new(org_reader.by_ref(), 16);

        let mut consumed = Vec::new();
        equal_reader.read_to_end(&mut consumed).unwrap();
        assert_eq!(consumed.len(), 16);

        drop(equal_reader);

        assert!(
            rx.recv_timeout(::std::time::Duration::from_millis(50)).is_err(),
            "a full drain must not send on last_read_signal"
        );
    }
}
