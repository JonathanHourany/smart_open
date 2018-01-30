import io
import six
import google
import logging
import mimetypes
from google.cloud import storage
from google.resumable_media.requests import ResumableUpload
from google.resumable_media import UPLOAD_CHUNK_SIZE
from google.cloud.storage.blob import _RESUMABLE_URL_TEMPLATE


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

START = 0
CURRENT = 1
END = 2
WHENCE_CHOICES = (START, CURRENT, END)

UPLOAD_URL = _RESUMABLE_URL_TEMPLATE.format(bucket_path='/b/{bucket_name}')
READ = 'r'
READ_BINARY = 'rb'
WRITE = 'w'
WRITE_BINARY = 'wb'
MODES = (WRITE_BINARY)


def open(bucket_name, blob_name, mode, **kwargs):
    logger.debug('%r', locals())
    if mode not in MODES:
        raise NotImplementedError('bad mode: %r expected one of %r' % (mode, MODES))

    encoding = kwargs.pop("encoding", "utf-8")
    errors = kwargs.pop("errors", None)
    newline = kwargs.pop("newline", None)
    line_buffering = kwargs.pop("line_buffering", False)
    upload_chunk_size = kwargs.pop("upload_chunk_size", UPLOAD_CHUNK_SIZE)

    # TODO: Ability to read from Google Storage
    # if mode in (READ, READ_BINARY):
    #     fileobj = SeekableBufferedInputBase(bucket_name, blob_name, **kwargs)
    if mode in (WRITE, WRITE_BINARY):
        fileobj = SeekableBufferedGCSOutputBase(bucket_name, blob_name, upload_chunk_size=upload_chunk_size, **kwargs)
    else:
        raise NotImplementedError("{mode} is not supported".format(mode=mode))

    # TODO: Add READ to mode
    if mode in (WRITE):
        return io.TextIOWrapper(fileobj, encoding=encoding, errors=errors,
                                newline=newline, line_buffering=line_buffering)
    # TODO: Add READ_BINARY to mode
    elif mode in (WRITE_BINARY):
        return fileobj
    else:
        raise NotImplementedError("{mode} is not supported".format(mode=mode))


class SeekableBufferedGCSOutputBase(io.BufferedIOBase):
    """Write bytes into GSC.
    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(self, bucket_name: str, blob_name: str, upload_chunk_size: int, **kwargs):
        content_type = kwargs.pop('content_type', mimetypes.guess_type(blob_name)[0])
        credentials = kwargs.pop('credentials', None)
        client = storage.Client(credentials=credentials)
        metadata = {'name': blob_name}

        try:
            bucket = client.get_bucket(bucket_name=bucket_name)
        except google.cloud.exceptions.NotFound:
            raise ValueError('the bucket {bucket} does not exist, or is forbidden for access'.format(bucket=bucket_name))

        self._transport = client._http
        self._upload_chunk_size = upload_chunk_size
        self._upload = ResumableUpload(UPLOAD_URL.format(bucket_name=bucket_name), self._upload_chunk_size)
        self._buf = io.BytesIO()
        self._upload_position = 0
        self._total_parts = 0
        self.raw = None

        self._upload.initiate(transport=self._transport, stream=self, metadata=metadata, content_type=content_type,
                              stream_final=False)

    # --------------------------------------
    # Function overrides from io.IOBase
    # --------------------------------------
    def close(self):
        logger.debug("closing")
        # Send everything left in the buffer
        while not self._upload.finished:
            self._upload_next_part()

        self._upload = None
        self._buf.close()

    @property
    def closed(self):
        return self._upload is None

    def writable(self):
        """Return True if the stream supports writing."""
        return True

    def tell(self):
        """Return the current stream position."""
        return self._upload_position

    def seek(self, offset, whence=START):
        """Change the stream position to the given byte offset. offset is interpreted relative to the position
        indicated by whence. The default value for whence is SEEK_SET. Values for whence are:

        SEEK_SET or 0 – start of the stream (the default); offset should be zero or positive
        SEEK_CUR or 1 – current stream position; offset may be negative
        SEEK_END or 2 – end of the stream; offset is usually negative
        Return the new absolute position.
        """
        delta = -1*self._buf.tell() + self._buf.seek(offset, whence)
        self._upload_position += delta
        return self.tell()

    def read(self, size=-1):
        data = self._buf.read(size)
        self._upload_position += len(data)
        return data

    # -------------------------------------------
    #  Function overrides from io.BufferedIOBase
    # -------------------------------------------
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    def write(self, b):
        """Write the given bytes (binary string) to the GCS file.
        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away."""
        if isinstance(b, six.string_types):
            b = bytearray(b, encoding='utf-8')

        logger.debug("writing %r bytes to %r", len(b), self._buf)

        self._buf.write(b)

        if self._buf.tell() >= self._upload_chunk_size:
            self._upload_next_part()

        return len(b)

    def terminate(self):
        """Cancel the underlying upload."""
        logger.debug("terminating")
        self._upload = None
        self._buf.close()

    # -----------------
    # Internal methods
    # ------------------
    def _upload_next_part(self):
        try:
            part_num = self._total_parts + 1
            logger.info("uploading part #%i, %i bytes (total %.3fGB)",
                        part_num, self._buf.tell(), self._upload.bytes_uploaded / 1024.0 ** 3)
            self._buf.seek(0)
            response = self._upload.transmit_next_chunk(self._transport)
            logger.debug("upload of part #%i finished" % part_num)
            logger.debug("http response: %s" % response)
            self._upload_position = self._upload.bytes_uploaded
            self._total_parts += 1
            self._buf = io.BytesIO(self._buf.read())
        except ValueError:
            if self._upload.finished:
                pass
            else:
                raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()
