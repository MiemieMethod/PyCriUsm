# python
from io import FileIO, BytesIO
from logging import getLogger
from pathlib import Path
from queue import SimpleQueue
from struct import Struct
from typing import Union

from .fast_core import UsmCrypter, HcaCrypter, FastUsmFile
from .key import get_key
from .util import reg_dict

logger = getLogger('PyCriUsm.Demuxer')
_usm_header_struct = Struct(r'>4sLxBHBxxBLL8x')
crypt_cache = ({}, {})


def get_crypter(key, is_hca=False):
    return reg_dict(crypt_cache[is_hca], key, lambda: (UsmCrypter, HcaCrypter)[is_hca](key))


def cleanup_cryptor():
    crypt_cache[0].clear()
    crypt_cache[1].clear()


def extract_usm(video_path, output, is_async: bool = False, **kwargs):
    video_path = Path(video_path)
    key_args = get_key(video_path)
    return demux(video_path, output, *key_args, is_async, **kwargs)


def demux(video_path, output: Union[str, Path, SimpleQueue], key=0, audio_encrypt=False, hca_encrypt=0,
          is_async=False, filter_mode=0, audio_chno=(), video_chno=()):
    """
    :param video_path: same meaning as arg name
    :param output: path-like object or queue.Queue
    :param key: usm decryption key, 0 means no encryption
    :param audio_encrypt: enable usm audio decryption
    :param hca_encrypt: 0 not provided, 1 same key as usm, other custom key
    :param filter_mode: param to FastUsmFile
    :param audio_chno: param to FastUsmFile
    :param video_chno: param to FastUsmFile
    :return: result (videos, audios) when output is path, or puts data into queue and returns marker when output is queue
    """
    video_path = Path(video_path)
    usm_file = FastUsmFile(video_path)
    usm_decrypter = None
    hca_decrypter = None
    if key:
        usm_decrypter = get_crypter(key, bool(hca_encrypt))
        if hca_encrypt and hca_encrypt != 0 and hca_encrypt != 1:
            hca_decrypter = get_crypter(hca_encrypt, True)
        elif hca_encrypt == 1:
            hca_decrypter = get_crypter(key, True)

    is_queue_output = isinstance(output, SimpleQueue)
    if not is_queue_output:
        output = Path(output)
        output.mkdir(parents=True, exist_ok=True)

    logger.info(f'start demux single-thread {video_path}')

    memory_cache = ({}, {})  # {is_video: {chno: BytesIO}}
    chunk_cache = [0, {}]  # [next_expected_index, {index: data}]
    stream_video_path = None
    stream_video = None
    max_index = None
    seen_any = False

    def write_cache(cache, data):
        buffer = reg_dict(cache, data.chno, BytesIO)
        write_file(buffer, data)

    def write_file_from_cache(cache, suffix):
        ret = {}
        for inno, buffer in cache.items():
            file_name = output / f'{video_path.stem}_{inno}{suffix}'
            ret[inno] = file_name
            with FileIO(file_name, 'wb') as f:
                f.write(buffer.getbuffer())
        return ret

    def write_file(fobj, data):
        nonlocal chunk_cache, max_index
        index = data.index
        if index == chunk_cache[0]:
            written = fobj.write(data)
            new_index = index + 1
            while True:
                nxt = chunk_cache[1].pop(new_index, None)
                if nxt is None:
                    break
                if fobj.write(nxt) != getattr(nxt, 'size', len(nxt)):
                    breakpoint()
                new_index += 1
            chunk_cache[0] = new_index
            if max_index is not None:
                if new_index == max_index:
                    if chunk_cache[1]:
                        raise ValueError("DEBUG: it seems that receiving is over but there's still some chunk in cache")
                elif new_index > max_index:
                    raise ValueError("DEBUG: number of chunks provided seems to be not as same as received")
        elif index > chunk_cache[0]:
            chunk_cache[1][index] = data
        for i in chunk_cache[1]:
            if i < chunk_cache[0]:
                raise ValueError("DEBUG:there're some logic problems in write_file")

    for buffer in usm_file.iter_chunks(filter_mode, audio_chno, video_chno):
        seen_any = True
        if buffer.is_video:
            if usm_decrypter and hasattr(usm_decrypter, 'decrypt_video'):
                usm_decrypter.decrypt_video(buffer)
        else:
            if hca_decrypter and hasattr(hca_decrypter, 'decrypt'):
                hca_decrypter.decrypt(buffer)
            elif audio_encrypt and usm_decrypter and hasattr(usm_decrypter, 'crypt_audio'):
                usm_decrypter.crypt_audio(buffer)

        if is_queue_output:
            output.put(buffer)
            continue

        if buffer.is_video and buffer.chno == 0:
            if stream_video_path is None:
                stream_video_path = output / (video_path.stem + '.ivf')
                stream_video = stream_video_path.open('wb')
            write_file(stream_video, buffer)
        else:
            write_cache(memory_cache[buffer.is_video], buffer)

    if seen_any:
        max_index = chunk_cache[0]
    else:
        max_index = 0

    if stream_video:
        stream_video.close()

    if is_queue_output:
        if seen_any:
            output.put(max_index)
        else:
            output.put(0)
        logger.info(f'{video_path} demux complete (queue mode)')
        return None

    if chunk_cache[1]:
        for idx in sorted(chunk_cache[1].keys()):
            data = chunk_cache[1][idx]
            write_cache(memory_cache[data.is_video], data)

    logger.debug(f'writing {video_path} finished')

    logger.debug('start writing audio cache to file')
    audios = write_file_from_cache(memory_cache[0], '.adx')
    logger.debug('start writing video cache to file')
    videos = write_file_from_cache(memory_cache[1], '.ivf')
    if stream_video_path:
        videos[0] = stream_video_path
    logger.info(f'{video_path} demux complete (file mode)')
    return videos, audios