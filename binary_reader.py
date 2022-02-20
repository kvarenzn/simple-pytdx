from typing import BinaryIO, Any
import struct
import io


class BinaryReader:
    _io: BinaryIO
    _big_endian: bool
    _file_size: int

    _SUPPORTED_TYPES = {
        'u8': 'B',
        'u16': 'H',
        'u32': 'I',
        'u64': 'Q',
        'i8': 'b',
        'i16': 'h',
        'i32': 'i',
        'i64': 'q',
        'f32': 'f',
        'f64': 'd'
    }

    def __init__(self, _io: BinaryIO, big_endian: bool = True):
        self._io = _io
        self._big_endian = big_endian
        self._file_size = self._io.seek(0, io.SEEK_END)
        self._io.seek(0, io.SEEK_SET)

    def read(self, size: int) -> bytes:
        return self._io.read(size)

    @property
    def eof(self) -> bool:
        return self._io.tell() >= self._file_size

    @property
    def pos(self) -> int:
        return self._io.tell()

    @pos.setter
    def pos(self, new_pos: int):
        self._io.seek(new_pos, io.SEEK_SET)

    def skip(self, size: int) -> int:
        return self._io.seek(size, io.SEEK_CUR)

    @property
    def vint(self) -> int:
        byte = self.u8
        result = byte & 0x3f
        sign = not bool(byte & 0x40)
        pos_byte = 6

        while byte & 0x80:
            byte = self.u8
            result += (byte & 0x7f) << pos_byte
            pos_byte += 7

        return result * (sign * 2 - 1)

    def str(self, encoding: str = 'gbk') -> str:
        result = bytearray()
        while char := self._io.read(1)[0]:
            result.append(char)
        return result.decode(encoding)

    def rpad_str(self, size: int, encoding: str = 'gbk') -> str:
        data = self._io.read(size)
        result = bytearray()
        for char in data:
            if not char:
                break
            result.append(char)
        return result.decode(encoding)

    def __getattr__(self, item: str) -> Any:
        if item in self._SUPPORTED_TYPES:
            fmt = self._SUPPORTED_TYPES[item]
            return struct.unpack('><'[self._big_endian] + fmt, self._io.read(struct.calcsize(fmt)))[0]
        raise AttributeError
