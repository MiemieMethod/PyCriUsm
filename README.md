# PyCriUsm
a python library to decrypt and demux HSR cutscene video. In theory, it can demux any of the encrypted Usm file. But I only adapted to HSR, GI and ZZZ..

The decryption module was written in cython so that we can achieve a speed 10x to 13x faster than WannaCRI and 1.4x faster than GI-Cutscene.

Refer to [WannaCRI](https://github.com/donmai-me/WannaCRI) and [GI-Cutscene](https://github.com/ToaHartor/GI-cutscenes)

This repository demuxes in a single thread. For multi-thread demuxing, please use the upstream [BUnipendix/PyCriUsm](https://github.com/BUnipendix/PyCriUsm), whose multi-thread demuxing has been fixed.

## Build Cython code

Enter the repository and run this command

```python setup.py build_ext --inplace```

## Example

```	
import asyncio
from pycriusm import extract_usm
videos, audios = asyncio.run(extract_usm(video_path, output_folder, is_async=True))
```

## Crack keys

When a video is not in keys.json, ``extract_usm`` tries to crack its key from the VP9 video itself. The cracker can also be used directly

```
from pycriusm.crack import crack_key, crack_keys
key, confidence = crack_key(video_path)
keys = crack_keys(video_folder)
```

GI videos encrypted with AES can not be cracked.

Thanks to Entity378 and failsafe65.

## Roadmap

- [x] USM decryption support
- [x] HCA decryption support(Used by GI and HSR 4.4+)
- [x] AES video decryption support(Used by HSR 4.4+)
- [ ] More flexable and powerful option to control usm chunks

