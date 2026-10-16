# python
"""
recover usm keys of VP9 videos (Genshin Impact, Zenless Zone Zero) by known plaintext of VP9 frames
"""
from array import array
from logging import getLogger
from pathlib import Path

from .demux import get_video_nonce
from .fast_core import FastUsmFile, get_video_mask, peel_video_chunk, solve_video_key

logger = getLogger('PyCriUsm.Crack')
_tile_cols_log2_cache = {}


class _BitReader:
	def __init__(self, data, start, readable):
		self.data = data
		self.start = start
		self.pos = start * 8
		self.readable = readable

	def read(self, n):
		value = 0
		for _ in range(n):
			byte_pos = self.pos >> 3
			if byte_pos >= len(self.data) or not self.readable(byte_pos):
				raise IndexError
			value = (value << 1) | (self.data[byte_pos] >> (7 - (self.pos & 7)) & 1)
			self.pos += 1
		return value

	def skip_optional(self, n):
		if self.read(1):
			self.read(n)


def _parse_vp9_header(reader, state):
	"""
	parse VP9 uncompressed header, return (header end, compressed header size, tile count)
	"""
	read = reader.read
	if read(2) != 2:
		raise ValueError('not a VP9 frame')
	profile = read(1)
	profile |= read(1) << 1
	if profile == 3:
		read(1)
	if read(1):
		# show_existing_frame
		return None
	frame_type = read(1)
	show_frame = read(1)
	error_resilient_mode = read(1)

	def color_config():
		if profile >= 2:
			read(1)
		if read(3) != 7:
			read(1)
			if profile in (1, 3):
				read(3)
		elif profile in (1, 3):
			read(1)

	def frame_size():
		state['width'] = read(16) + 1
		read(16)

	if frame_type == 0:
		if read(24) != 0x498342:
			raise ValueError('wrong frame sync code')
		color_config()
		frame_size()
		reader.skip_optional(32)
	else:
		intra_only = 0 if show_frame else read(1)
		if not error_resilient_mode:
			read(2)
		if intra_only:
			if read(24) != 0x498342:
				raise ValueError('wrong frame sync code')
			if profile > 0:
				color_config()
			read(8)
			frame_size()
			reader.skip_optional(32)
		else:
			read(8)
			read(12)
			for _ in range(3):
				if read(1):
					break
			else:
				frame_size()
			reader.skip_optional(32)
			read(1)
			if not read(1):
				read(2)
	if not error_resilient_mode:
		read(2)
	read(2)
	# loop filter
	read(9)
	if read(1) and read(1):
		for _ in range(6):
			reader.skip_optional(7)
	# quantization
	read(8)
	for _ in range(3):
		reader.skip_optional(5)
	# segmentation
	if read(1):
		if read(1):
			for _ in range(7):
				reader.skip_optional(8)
			if read(1):
				for _ in range(3):
					reader.skip_optional(8)
		if read(1):
			read(1)
			for _ in range(8):
				for bits in (9, 7, 2, 0):
					reader.skip_optional(bits)
	# tile info
	if 'width' not in state:
		raise ValueError('unknown frame width')
	min_log2, max_log2 = _get_tile_cols_log2(state['width'])
	tile_cols_log2 = min_log2
	while tile_cols_log2 < max_log2 and read(1):
		tile_cols_log2 += 1
	tile_rows_log2 = read(1)
	if tile_rows_log2:
		tile_rows_log2 += read(1)
	header_size = read(16)
	return ((reader.pos + 7) >> 3) - reader.start, header_size, 1 << (tile_cols_log2 + tile_rows_log2)


def _get_tile_cols_log2(width):
	ret = _tile_cols_log2_cache.get(width)
	if ret is None:
		sb64_cols = (((width + 7) >> 3) + 7) >> 3
		min_log2 = 0
		while (64 << min_log2) < sb64_cols:
			min_log2 += 1
		max_log2 = 1
		while (sb64_cols >> max_log2) >= 4:
			max_log2 += 1
		ret = _tile_cols_log2_cache[width] = (min_log2, max_log2 - 1)
	return ret


def _iter_known_plain(data, readable, state):
	"""
	yield (offset, plain byte, weight) that VP9 frames almost always have
	"""
	start = 32 if data[:4] == b'DKIF' else 0
	if len(data) < start + 12 or int.from_bytes(data[start:start + 4], 'little') != len(data) - start - 12:
		return
	start += 12
	end = len(data)
	try:
		parsed = _parse_vp9_header(_BitReader(data, start, readable), state)
	except (IndexError, ValueError):
		return
	if parsed:
		header_end, header_size, tile_num = parsed
		tile_start = start + header_end + header_size
		if header_size and tile_start <= end:
			# the boolean decoder of compressed header ends with zero padding
			yield tile_start - 1, 0, 1
		if tile_num > 1 and tile_start + 2 <= end:
			# big endian size of the first tile
			yield tile_start, 0, 1
			yield tile_start + 1, 0, 1
	if not readable(end - 1):
		# the last tile usually ends with zero padding, if it is not a superframe
		yield end - 1, 0, 0.5
		return
	# superframe index
	marker = data[end - 1]
	if marker & 0xe0 != 0xc0:
		return
	bytes_per_size = ((marker >> 3) & 3) + 1
	frame_num = (marker & 7) + 1
	index_start = end - 2 - bytes_per_size * frame_num
	if index_start <= start:
		return
	yield index_start, marker, 1
	if all(readable(i) for i in range(index_start + 1, index_start + 1 + bytes_per_size)):
		first_size = int.from_bytes(data[index_start + 1:index_start + 1 + bytes_per_size], 'little')
		if start + first_size < index_start:
			# the second frame is usually a shown inter frame
			yield start + first_size - 1, 0, 0.5
			yield start + first_size, 0x86, 1
	yield index_start - 1, 0, 0.5


class _PlainView:
	"""
	a peeled video chunk decrypted by a candidate mask, only the bytes read are decrypted
	"""
	def __init__(self, data, kind, mask):
		self.data = data
		self.kind = kind
		self.masks = (bytes(0x20), mask, bytes(i ^ 0xff for i in mask))

	def __len__(self):
		return len(self.data)

	def __getitem__(self, item):
		if isinstance(item, slice):
			return bytes(self[i] for i in range(*item.indices(len(self.data))))
		return self.data[item] ^ self.masks[self.kind[item]][item & 0x1f]


def _always(_):
	return True


def _check_structure(data, state):
	"""
	count VP9 frames whose header, tile sizes and superframe index are consistent, return (good, total)
	"""
	start = (32 if data[:4] == b'DKIF' else 0) + 12
	end = len(data)
	frames = [(start, end)]
	marker = data[end - 1]
	if marker & 0xe0 == 0xc0:
		bytes_per_size = ((marker >> 3) & 3) + 1
		frame_num = (marker & 7) + 1
		index_start = end - 2 - bytes_per_size * frame_num
		if index_start > start and data[index_start] == marker:
			index = data[index_start + 1:end - 1]
			frames = []
			pos = start
			for i in range(0, len(index), bytes_per_size):
				size = int.from_bytes(index[i:i + bytes_per_size], 'little')
				frames.append((pos, pos + size))
				pos += size
			if pos != index_start:
				return 0, frame_num
	good = 0
	for frame_start, frame_end in frames:
		try:
			parsed = _parse_vp9_header(_BitReader(data, frame_start, _always), state)
		except (IndexError, ValueError):
			continue
		if parsed is None:
			# show_existing_frame
			good += frame_end - frame_start <= 2
			continue
		header_end, header_size, tile_num = parsed
		pos = frame_start + header_end
		# the marker bit of every boolean decoder must be zero
		if not header_size or pos + header_size > frame_end or data[pos] & 0x80:
			continue
		pos += header_size
		for i in range(tile_num):
			size = frame_end - pos
			if i < tile_num - 1:
				if pos + 4 > frame_end:
					break
				size = int.from_bytes(data[pos:pos + 4], 'big')
				pos += 4
			if size <= 0 or pos + size > frame_end or data[pos] & 0x80:
				break
			pos += size
		else:
			good += 1
	return good, len(frames)


def _structure_score(samples, key, state):
	mask = get_video_mask(key)
	good = 0
	total = 0
	for data, kind in samples:
		ret = _check_structure(_PlainView(data, kind, mask), dict(state))
		good += ret[0]
		total += ret[1]
	return good, total


def _get_structure_score(samples, key, state):
	good, total = _structure_score(samples, key, state)
	return good * 2 - total


def _get_lanes_of_key_byte():
	ret = []
	for i in range(7):
		base = get_video_mask(0)
		ret.append({lane for value in (0x55, 0xaa) for lane, (a, b) in enumerate(zip(base, get_video_mask(value << i * 8))) if a != b})
	return ret


_lanes_of_key_byte = _get_lanes_of_key_byte()


def _get_vote_score(votes, key):
	mask = get_video_mask(key)
	return sum(votes[i << 8 | mask[i]] for i in range(0x20))


def _repair_key(key, votes, samples, state):
	"""
	key bytes whose lanes lack known plaintext are chosen by VP9 structure, then by votes
	return (key, max number of values that fit equally well for a key byte)
	"""
	mask = get_video_mask(key)
	weak_lanes = {i for i in range(0x20) if votes[i << 8 | mask[i]] < 2}
	weak_bytes = [i for i, lanes in enumerate(_lanes_of_key_byte) if lanes & weak_lanes]
	ties = 1
	for _ in range(3):
		changed = False
		ties = 1
		for i in weak_bytes:
			shift = i * 8
			scores = [(_get_structure_score(samples, candidate, state), _get_vote_score(votes, candidate))
					  for candidate in (key & ~(0xff << shift) | value << shift for value in range(0x100))]
			best = max(scores)
			# values that votes can not tell apart from the best one
			ties = max(ties, sum(i[0] == best[0] and i[1] > best[1] - 2 for i in scores))
			if scores[key >> shift & 0xff] < best:
				key = key & ~(0xff << shift) | scores.index(best) << shift
				changed = True
		if not changed:
			break
	return key, ties


def _read_video_chunks(video_path, max_chunks):
	usm_file = FastUsmFile(video_path)
	chunks = []
	try:
		for chunk in usm_file.iter_chunks():
			if not chunk.is_video or chunk.chno != 0:
				continue
			chunks.append(chunk)
			if len(chunks) >= max_chunks:
				break
	finally:
		usm_file.close()
	return chunks


def crack_key(video_path, max_chunks=2000, repair_samples=30):
	"""
	:return: (key, confidence), key 0 means no encryption, None means not crackable
	"""
	video_path = Path(video_path)
	if get_video_nonce(video_path):
		logger.warning(f'{video_path.name} uses AES encryption, which can not be cracked')
		return None, 0
	chunks = _read_video_chunks(video_path, max_chunks)
	if not chunks or bytes(chunks[0])[:4] != b'DKIF':
		logger.warning(f'{video_path.name} is not a VP9 usm')
		return None, 0
	state = {}
	good, total = 0, 0
	for chunk in chunks:
		if len(chunk) < 0x240:
			continue
		ret = _check_structure(bytes(chunk), state)
		good += ret[0]
		total += ret[1]
	if good > 0.9 * total:
		return 0, good / total

	votes = array('d', bytes(8 * 0x20 * 0x100))
	state = {}
	peeled = []
	for chunk in chunks:
		data, kind = peel_video_chunk(chunk)
		peeled.append((data, kind))
		for offset, value, weight in _iter_known_plain(data, lambda x: kind[x] == 0, state):
			if kind[offset] == 1:
				votes[(offset & 0x1f) << 8 | (data[offset] ^ value)] += weight
			elif kind[offset] == 2:
				votes[(offset & 0x1f) << 8 | (data[offset] ^ value ^ 0xff)] += weight
	candidates = solve_video_key(memoryview(votes).cast('B').cast('d', (0x20, 0x100)))
	key = candidates[0][0]
	confidence = 0
	if 'width' in state:
		state = {'width': state['width']}
		peeled = [i for i in peeled if len(i[0]) >= 0x240]
		samples = peeled[::max(len(peeled) // repair_samples, 1)][:repair_samples]
		# votes of a short video may be not enough, so let VP9 structure decide among the candidates
		key = max(candidates, key=lambda x: (_get_structure_score(samples, x[0], state), x[1]))[0]
		key, ties = _repair_key(key, votes, samples, state)
		good, total = _structure_score(peeled[:max_chunks // 4], key, state)
		# more than one key fits, the key is a guess
		confidence = good / total / ties if total else 0
	logger.info(f'{video_path.name} key: {key}, confidence: {confidence:.3f}')
	return key, confidence


def crack_keys(video_dir, min_confidence=0.9):
	"""
	crack keys of all usm in video_dir, return {video name: key}
	"""
	keys = {}
	for video_path in sorted(Path(video_dir).rglob('*.usm')):
		key, confidence = crack_key(video_path)
		if not key:
			continue
		if confidence < min_confidence:
			logger.warning(f'{video_path.name} low confidence: {confidence:.3f}, skipped')
			continue
		if keys.get(video_path.stem, key) != key:
			logger.warning(f'{video_path.name} has the same name as another video but a different key, skipped')
			continue
		keys[video_path.stem] = key
	return keys
