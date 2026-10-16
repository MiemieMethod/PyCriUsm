from libc.string cimport memcpy, memset
from cython cimport boundscheck, wraparound
from cpython cimport PyMem_Malloc, PyMem_Free, PyObject
from cpython cimport PyLong_FromLong
from libc.stdio cimport fdopen, fseek, ftell, fread, rewind, ferror, setvbuf, _IOFBF, FILE, printf
from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int32_t, int64_t


cdef uint16_t[256] hca_check_sum_table = [
	0x0000, 0x8005, 0x800F, 0x000A, 0x801B, 0x001E, 0x0014, 0x8011,
	0x8033, 0x0036, 0x003C, 0x8039, 0x0028, 0x802D, 0x8027, 0x0022,
	0x8063, 0x0066, 0x006C, 0x8069, 0x0078, 0x807D, 0x8077, 0x0072,
	0x0050, 0x8055, 0x805F, 0x005A, 0x804B, 0x004E, 0x0044, 0x8041,
	0x80C3, 0x00C6, 0x00CC, 0x80C9, 0x00D8, 0x80DD, 0x80D7, 0x00D2,
	0x00F0, 0x80F5, 0x80FF, 0x00FA, 0x80EB, 0x00EE, 0x00E4, 0x80E1,
	0x00A0, 0x80A5, 0x80AF, 0x00AA, 0x80BB, 0x00BE, 0x00B4, 0x80B1,
	0x8093, 0x0096, 0x009C, 0x8099, 0x0088, 0x808D, 0x8087, 0x0082,
	0x8183, 0x0186, 0x018C, 0x8189, 0x0198, 0x819D, 0x8197, 0x0192,
	0x01B0, 0x81B5, 0x81BF, 0x01BA, 0x81AB, 0x01AE, 0x01A4, 0x81A1,
	0x01E0, 0x81E5, 0x81EF, 0x01EA, 0x81FB, 0x01FE, 0x01F4, 0x81F1,
	0x81D3, 0x01D6, 0x01DC, 0x81D9, 0x01C8, 0x81CD, 0x81C7, 0x01C2,
	0x0140, 0x8145, 0x814F, 0x014A, 0x815B, 0x015E, 0x0154, 0x8151,
	0x8173, 0x0176, 0x017C, 0x8179, 0x0168, 0x816D, 0x8167, 0x0162,
	0x8123, 0x0126, 0x012C, 0x8129, 0x0138, 0x813D, 0x8137, 0x0132,
	0x0110, 0x8115, 0x811F, 0x011A, 0x810B, 0x010E, 0x0104, 0x8101,
	0x8303, 0x0306, 0x030C, 0x8309, 0x0318, 0x831D, 0x8317, 0x0312,
	0x0330, 0x8335, 0x833F, 0x033A, 0x832B, 0x032E, 0x0324, 0x8321,
	0x0360, 0x8365, 0x836F, 0x036A, 0x837B, 0x037E, 0x0374, 0x8371,
	0x8353, 0x0356, 0x035C, 0x8359, 0x0348, 0x834D, 0x8347, 0x0342,
	0x03C0, 0x83C5, 0x83CF, 0x03CA, 0x83DB, 0x03DE, 0x03D4, 0x83D1,
	0x83F3, 0x03F6, 0x03FC, 0x83F9, 0x03E8, 0x83ED, 0x83E7, 0x03E2,
	0x83A3, 0x03A6, 0x03AC, 0x83A9, 0x03B8, 0x83BD, 0x83B7, 0x03B2,
	0x0390, 0x8395, 0x839F, 0x039A, 0x838B, 0x038E, 0x0384, 0x8381,
	0x0280, 0x8285, 0x828F, 0x028A, 0x829B, 0x029E, 0x0294, 0x8291,
	0x82B3, 0x02B6, 0x02BC, 0x82B9, 0x02A8, 0x82AD, 0x82A7, 0x02A2,
	0x82E3, 0x02E6, 0x02EC, 0x82E9, 0x02F8, 0x82FD, 0x82F7, 0x02F2,
	0x02D0, 0x82D5, 0x82DF, 0x02DA, 0x82CB, 0x02CE, 0x02C4, 0x82C1,
	0x8243, 0x0246, 0x024C, 0x8249, 0x0258, 0x825D, 0x8257, 0x0252,
	0x0270, 0x8275, 0x827F, 0x027A, 0x826B, 0x026E, 0x0264, 0x8261,
	0x0220, 0x8225, 0x822F, 0x022A, 0x823B, 0x023E, 0x0234, 0x8231,
	0x8213, 0x0216, 0x021C, 0x8219, 0x0208, 0x820D, 0x8207, 0x0202
]


cdef Py_ssize_t _char_size = sizeof(char)


#无符号整型16位
@boundscheck(False)
cdef uint16_t _bswap_16(uint16_t* x) noexcept nogil:
	return ((x[0] & 0x00ff) << 8) | ((x[0] & 0xff00) >> 8)
 

#无符号整型32位
@boundscheck(False)
cdef uint32_t _bswap_32(uint32_t* x) noexcept nogil:
	return ((x[0] & (<uint32_t>0xff000000)) >> 24) |  ((x[0] & (<uint32_t>0x00ff0000)) >> 8) |  ((x[0] & (<uint32_t>0x0000ff00)) << 8) |  ((x[0] & (<uint32_t>0x000000ff)) << 24)


cdef void load_array(iter_obj, uint8_t* array):
	cdef uint8_t i
	for i in iter_obj:
		array[0] = i
		array = array + 1


def test_for_array(iter_obj):
	cdef uint8_t[64] array
	load_array(iter_obj, array)
	return array


@boundscheck(False)
cdef bint _is_in_array(uint8_t num, uint8_t* array, Py_ssize_t array_len) noexcept nogil:
	cdef bint result = 0
	for i in range(array_len):
		if num == array[i]:
			result = 1
	return result


cdef class SimpleBuffer:
	cdef readonly Py_ssize_t size
	cdef uint8_t* data
	cdef readonly bint is_video
	cdef readonly uint8_t chno
	cdef readonly uint32_t index
	cdef readonly uint32_t frame_time
	cdef int64_t buffer_ref_count

	def __cinit__(self):
		self.size = 0

	cdef void init(self, size_t size):
		self.free()
		self.data = <uint8_t*>PyMem_Malloc(size)
		self.size = size

	def __getbuffer__(self, Py_buffer *buffer, int flags):
		buffer.buf = self.data
		buffer.format = "B"
		buffer.internal = NULL
		buffer.itemsize = sizeof(char)
		buffer.len = self.size
		buffer.ndim = 1
		buffer.obj = self
		buffer.readonly = 0
		buffer.shape = &self.size
		buffer.strides = &_char_size
		buffer.suboffsets = NULL
		self.buffer_ref_count += 1

	def __releasebuffer__(self, Py_buffer *buffer):
		self.buffer_ref_count -= 1
		if self.buffer_ref_count < 0:
			raise OverflowError('buffer ref count become minus, something wrong')

	def __len__(self):
		return self.size

	cdef void free(self):
		if self.data:
			# printf("call simple buffer free")
			PyMem_Free(<void*>self.data)
			self.data = NULL

	def __dealloc__(self):
		self.free()


cdef class FastUsmFile:
	cdef FILE* _file_obj
	cdef _py_file_obj
	cdef char* _custom_buf
	cdef uint32_t chunk_index

	def __cinit__(self, path):
		from io import FileIO, DEFAULT_BUFFER_SIZE
		self._py_file_obj = FileIO(path, "rb")
		self._file_obj = fdopen(self._py_file_obj.fileno(), "rb")
		self._custom_buf = <char*>PyMem_Malloc(<size_t>DEFAULT_BUFFER_SIZE)
		setvbuf(self._file_obj, self._custom_buf, _IOFBF, 8192)

	cdef SimpleBuffer _get_chunk(self, int32_t offset, size_t size, bint is_video, uint8_t chno, uint32_t frame_time):
		cdef SimpleBuffer buffer = SimpleBuffer()
		buffer.init(size)
		with nogil:
			buffer.is_video = is_video
			buffer.chno = chno
			buffer.frame_time = frame_time
			buffer.index = self.chunk_index
			self.chunk_index += 1
			fseek(self._file_obj, offset, 1)
			fread(buffer.data, 1, size, self._file_obj)
			self._check_file_error()
		return buffer

	@boundscheck(False)
	@wraparound(False)
	def iter_chunks(self, int8_t filter_mode=0, filter_audio_chnos=(), filter_video_chnos=()):
		cdef uint8_t[24] header_array
		cdef uint32_t file_size, pos, chunk_size
		cdef uint32_t* chunk_type
		cdef uint16_t padding_size
		cdef uint8_t* data_offset
		cdef uint8_t* chno
		cdef uint8_t data_type
		cdef bint is_video

		# printf("进入iter_chunks函数成功\n")
		cdef uint8_t filter_audio_len = len(filter_audio_chnos)
		cdef uint8_t filter_video_len = len(filter_video_chnos)
		cdef uint8_t[64] _filter_audio_chnos
		cdef uint8_t[8] _filter_video_chnos
		cdef bint enable_filter

		# 检查参数
		# printf("参数检查\n")
		if len(filter_audio_chnos) > 64:
			raise OverflowError("最多支持64个Audio Chno")
		if len(filter_video_chnos) > 8:
			raise OverflowError("最多支持8个Video Chno")
		if filter_mode > 2 or filter_mode < 0:
			raise TypeError('Wrong filter_mode arg:%s'%filter_mode)

		with nogil:
			# printf("预处理\n")
			enable_filter = 0 if filter_mode == 0 else 1
			if filter_mode > 0:
				filter_mode -= 1
			self.chunk_index = 0
			chunk_type = <uint32_t*>header_array
			data_offset = <uint8_t*>header_array + 9
			chno = <uint8_t*>header_array + 12
			fseek(self._file_obj, 0, 2)
			file_size = ftell(self._file_obj)
			rewind(self._file_obj)

			while pos < file_size:
				# printf("开始读取头\n")
				fseek(self._file_obj, pos, 0)
				fread(header_array, 1, sizeof(header_array), self._file_obj)
				self._check_file_error()
				# printf("解析头\n")
				chunk_size = _bswap_32(<uint32_t*>(header_array + 4))
				padding_size = _bswap_16(<uint16_t*>(header_array + 10))
				pos += chunk_size + 8
				data_type = (header_array[15]) & 3
				# with gil:
				#	print(header_array[:sizeof(header_array)])
					# print("header信息：", chunk_size, data_offset[0], padding_size, pos, data_type)
				if data_type != 0:
					#with gil:
					#	print('非流，跳过\n')
					continue
				if chunk_type[0] == 1447449408:
					if enable_filter and filter_mode^_is_in_array(chno[0], _filter_video_chnos, filter_video_len):
						#with gil:
						#	print('筛选，跳过\n')
						continue
					is_video = 1
				elif chunk_type[0] == 1095127872:
					if enable_filter and filter_mode^_is_in_array(chno[0], _filter_audio_chnos, filter_audio_len):
						#with gil:
						#	print('筛选，跳过\n')
						continue
					is_video = 0
				else:
					#with gil:
					#	print('未知chunk，跳过\n')
					continue
				# printf("开始读取chunk\n")
				with gil:
					yield self._get_chunk(data_offset[0] + 8 - sizeof(header_array), chunk_size - data_offset[0] - padding_size, is_video, chno[0], _bswap_32(<uint32_t*>(header_array + 16)))
				#printf('proccess end\n')
			self.chunk_index = 0

	def __dealloc__(self):
		self.close()
		PyMem_Free(<void*>self._custom_buf)

	cdef void _check_file_error(self) noexcept nogil:
		# printf("检查文件错误\n")
		if ferror(self._file_obj):
			with gil:
				raise IOError

	cpdef void close(self):
		self._py_file_obj.close()


@boundscheck(False)
cdef void _init_video_mask(uint64_t key, uint8_t* m) noexcept nogil:
	cdef uint32_t tmp_key1
	cdef uint32_t tmp_key2
	cdef uint8_t* key1
	cdef uint8_t* key2
	tmp_key1 = key & (<uint32_t>0xffffffff)
	tmp_key2 = key >> 32
	key1 = <uint8_t*>&tmp_key1
	key2 = <uint8_t*>&tmp_key2
	m[0x00] = key1[0]
	m[0x01] = key1[1]
	m[0x02] = key1[2]
	m[0x03] = key1[3] - 0x34
	m[0x04] = key2[0] + 0xF9
	m[0x05] = key2[1] ^ 0x13
	m[0x06] = key2[2] + 0x61
	m[0x07] = m[0x00] ^ 0xFF
	m[0x08] = m[0x02] + m[0x01]
	m[0x09] = m[0x01] - m[0x07]
	m[0x0A] = m[0x02] ^ 0xFF
	m[0x0B] = m[0x01] ^ 0xFF
	m[0x0C] = m[0x0B] + m[0x09]
	m[0x0D] = m[0x08] - m[0x03]
	m[0x0E] = m[0x0D] ^ 0xFF
	m[0x0F] = m[0x0A] - m[0x0B]
	m[0x10] = m[0x08] - m[0x0F]
	m[0x11] = m[0x10] ^ m[0x07]
	m[0x12] = m[0x0F] ^ 0xFF
	m[0x13] = m[0x03] ^ 0x10
	m[0x14] = m[0x04] - 0x32
	m[0x15] = m[0x05] + 0xED
	m[0x16] = m[0x06] ^ 0xF3
	m[0x17] = m[0x13] - m[0x0F]
	m[0x18] = m[0x15] + m[0x07]
	m[0x19] = 0x21 - m[0x13]
	m[0x1A] = m[0x14] ^ m[0x17]
	m[0x1B] = m[0x16] + m[0x16]
	m[0x1C] = m[0x17] + 0x44
	m[0x1D] = m[0x03] + m[0x04]
	m[0x1E] = m[0x05] - m[0x16]
	m[0x1F] = m[0x1D] ^ m[0x13]


cdef class UsmCrypter:
	cdef uint8_t[0x20] _video_mask1
	cdef uint8_t[0x20] _video_mask2
	cdef uint8_t[0x20] _audio_mask

	@wraparound(False)
	@boundscheck(False)
	def __init__(self, uint64_t key):
		cdef uint8_t[4] table2
		cdef uint8_t i

		with nogil:
			table2 = [85, 82, 85, 67]
			_init_video_mask(key, self._video_mask1)

			for i in range(0x20):
				self._video_mask2[i] = self._video_mask1[i] ^ 0xFF
				self._audio_mask[i] = table2[i >> 1 & 3] if i & 1 == 1 else self._video_mask1[i] ^ 0xFF

	@wraparound(False)
	@boundscheck(False)
	def decrypt_video(self, SimpleBuffer chunk_array):
		cdef uint64_t i
		cdef uint64_t dataOffset = 0x40
		cdef uint8_t[0x20] mask
		cdef uint8_t* data = chunk_array.data
		cdef Py_ssize_t size = chunk_array.size
		if size < 0x240:
			return
		with nogil:
			size -= dataOffset
			memcpy(mask, self._video_mask2, sizeof(mask))
			for i in range(0x100, size):
				data[i + dataOffset] ^= mask[i & 0x1F]
				mask[i & 0x1F] = data[i + dataOffset] ^ self._video_mask2[i & 0x1F]
			memcpy(mask, self._video_mask1, sizeof(mask))
			for i in range(0x100):
				mask[i & 0x1F] ^= data[0x100 + i + dataOffset]
				data[i + dataOffset] ^= mask[i & 0x1F]

	@wraparound(False)
	@boundscheck(False)
	def crypt_audio(self, SimpleBuffer chunk_array):
		cdef uint64_t i
		cdef uint8_t* data = chunk_array.data
		cdef Py_ssize_t size = chunk_array.size
		if size <= 0x140:
			return
		with nogil:
			for i in range(0x140, size):
				data[i] ^= self._audio_mask[i & 0x1f]


@wraparound(False)
@boundscheck(False)
def get_video_mask(uint64_t key):
	cdef uint8_t[0x20] mask
	_init_video_mask(key, mask)
	return <bytes>mask[:0x20]


@wraparound(False)
@boundscheck(False)
def peel_video_chunk(SimpleBuffer chunk_array):
	"""
	remove the key independent part of usm video encryption, return (data, kind)
	kind 0: data is plain, 1: plain = data ^ video_mask1, 2: plain = data ^ ~video_mask1
	"""
	cdef Py_ssize_t size = chunk_array.size
	cdef uint8_t* data = chunk_array.data
	cdef bytearray py_out = bytearray(size)
	cdef bytearray py_kind = bytearray(size)
	cdef uint8_t* out = py_out
	cdef uint8_t* kind = py_kind
	cdef uint8_t[0x20] acc
	cdef Py_ssize_t i
	cdef Py_ssize_t row
	if size == 0:
		return py_out, py_kind
	memcpy(out, data, size)
	if size < 0x240:
		return py_out, py_kind
	with nogil:
		# from 0x140, every byte is chained with the byte 0x20 ahead
		memset(acc, 0, sizeof(acc))
		for i in range(0x140, size):
			acc[i & 0x1F] ^= data[i]
			out[i] = acc[i & 0x1F]
			kind[i] = 2 if ((i - 0x140) >> 5) & 1 == 0 else 0
		# 0x40 - 0x140 is masked by the running xor of plain bytes from 0x140
		memset(acc, 0, sizeof(acc))
		for row in range(8):
			for i in range(0x20):
				acc[i] ^= out[0x140 + row * 0x20 + i]
				out[0x40 + row * 0x20 + i] = data[0x40 + row * 0x20 + i] ^ acc[i]
				if row & 2:
					kind[0x40 + row * 0x20 + i] = 1
				else:
					out[0x40 + row * 0x20 + i] ^= 0xFF
	return py_out, py_kind


cdef uint8_t[13] _lanes_k012 = [0x00, 0x01, 0x02, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0F, 0x10, 0x11, 0x12]
cdef uint8_t[12] _lanes_k34 = [0x03, 0x04, 0x0D, 0x0E, 0x13, 0x14, 0x17, 0x19, 0x1A, 0x1C, 0x1D, 0x1F]
cdef uint8_t[7] _lanes_k56 = [0x05, 0x06, 0x15, 0x16, 0x18, 0x1B, 0x1E]


@wraparound(False)
@boundscheck(False)
cdef double _score_lanes(double[:, ::1] votes, uint64_t key, uint8_t* lanes, int lane_num) noexcept nogil:
	cdef uint8_t[0x20] mask
	cdef double score = 0
	cdef int i
	_init_video_mask(key, mask)
	for i in range(lane_num):
		score += votes[lanes[i], mask[lanes[i]]]
	return score


cdef void _insert_top(double* scores, uint64_t* keys, int num, double score, uint64_t key) noexcept nogil:
	cdef int j = num - 1
	if score <= scores[j]:
		return
	while j > 0 and scores[j - 1] < score:
		scores[j] = scores[j - 1]
		keys[j] = keys[j - 1]
		j -= 1
	scores[j] = score
	keys[j] = key


@wraparound(False)
@boundscheck(False)
def solve_video_key(double[:, ::1] votes, int candidates=32):
	"""
	find the usm keys whose video_mask1 fit the votes (votes[lane][mask byte]) best
	key bytes 0-2, 3-4 and 5-6 decide different lanes of the mask, so they are searched by stage
	return [(key, score)] of the best candidates, the best first
	"""
	cdef uint64_t[32] keys_k012
	cdef double[32] scores_k012
	cdef uint64_t[8] keys_k34
	cdef double[8] scores_k34
	cdef uint64_t[8] keys_k56
	cdef double[8] scores_k56
	cdef uint64_t[32] result_keys
	cdef double[32] result_scores
	cdef uint64_t key
	cdef uint64_t i
	cdef int j
	cdef int k
	cdef int l
	if votes.shape[0] != 0x20 or votes.shape[1] != 0x100:
		raise ValueError('votes must be 32 * 256')
	candidates = min(max(candidates, 1), 32)
	with nogil:
		for j in range(candidates):
			scores_k012[j] = -1
			result_scores[j] = -1
		for key in range(0x1000000):
			_insert_top(scores_k012, keys_k012, candidates, _score_lanes(votes, key, _lanes_k012, 13), key)
		for k in range(candidates):
			if scores_k012[k] < 0:
				break
			for j in range(8):
				scores_k34[j] = -1
				scores_k56[j] = -1
			for i in range(0x10000):
				_insert_top(scores_k34, keys_k34, 8, _score_lanes(votes, keys_k012[k] | (i << 24), _lanes_k34, 12), i << 24)
				_insert_top(scores_k56, keys_k56, 8, _score_lanes(votes, keys_k012[k] | (i << 40), _lanes_k56, 7), i << 40)
			for j in range(8):
				for l in range(8):
					if scores_k34[j] >= 0 and scores_k56[l] >= 0:
						_insert_top(result_scores, result_keys, candidates, scores_k012[k] + scores_k34[j] + scores_k56[l], keys_k012[k] | keys_k34[j] | keys_k56[l])
	return [(result_keys[j], result_scores[j]) for j in range(candidates) if result_scores[j] >= 0]


@wraparound(False)
@boundscheck(False)
cdef void init_default_audio_ciph(uint8_t* _ciphTable):
	cdef uint64_t i
	cdef uint8_t v = 0
	for i in range(1, 0xff):
		v = v * 13 + 11
		if v == 0 or v == 0xFF:
			v = v * 13 + 11
		_ciphTable[i] = v
	_ciphTable[0] = 0
	_ciphTable[0xFF] = 0xFF

cdef uint8_t[0x100] default_audio_ciph_table
init_default_audio_ciph(default_audio_ciph_table)


cdef inline bint _is_hca_tag(uint8_t* data, const char* tag) noexcept nogil:
	# chunk names of hca header may be masked by 0x80
	return (data[0] & 0x7F) == tag[0] and (data[1] & 0x7F) == tag[1] and (data[2] & 0x7F) == tag[2] and (data[3] & 0x7F) == tag[3]


cdef class HcaCrypter:
	cdef uint8_t[0x100] _ciph_table
	cdef uint16_t _cipher_type
	cdef uint16_t _block_size

	@wraparound(False)
	@boundscheck(False)
	def __init__(self, uint64_t key):
		cdef uint64_t i
		cdef uint64_t j
		cdef uint8_t tmp1
		cdef uint8_t v = 0
		cdef uint8_t[7] t1
		cdef uint8_t[16] t2
		cdef uint8_t[0x100] t3
		cdef uint8_t[0x10] t31
		cdef uint8_t[0x10] t32
		cdef uint32_t iTable = 1

		self._cipher_type = 0
		self._block_size = 0
		if key:
			key -= 1
		for i in range(7):
			t1[i] = key & 0xFF
			key >>= 8

		t2 = [t1[1], t1[1] ^ t1[6], t1[2] ^ t1[3],
			t1[2], t1[2] ^ t1[1], t1[3] ^ t1[4],
			t1[3], t1[3] ^ t1[2], t1[4] ^ t1[5],
			t1[4], t1[4] ^ t1[3], t1[5] ^ t1[6],
			t1[5], t1[5] ^ t1[4], t1[6] ^ t1[1],
			t1[6]]

		init56_create_table(t31, t1[0])
		# Create Table
		for i in range(0x10):
			init56_create_table(t32, t2[i])
			for j in range(0x10):
				t3[i * 0x10 + j] = (t31[i] << 4) | t32[j]

		# CIPHテーブル
		for i in range(0x100):
			v = v + 0x11
			tmp1 = t3[v]
			if tmp1 != 0 and tmp1 != 0xFF:
				self._ciph_table[iTable] = tmp1
				iTable += 1
		self._ciph_table[0] = 0
		self._ciph_table[0xFF] = 0xFF

	@wraparound(False)
	@boundscheck(False)
	def decrypt(self, SimpleBuffer chunk_array):
		"""
		decrypt a usm audio chunk in place, which is either the hca header or some whole hca frames
		"""
		cdef uint8_t* data = chunk_array.data
		cdef uint64_t size = chunk_array.size
		cdef uint64_t i
		cdef uint64_t pos
		cdef uint64_t header_size
		cdef uint64_t block_size
		cdef uint8_t* _ciph_table
		if size < 8:
			return
		if _is_hca_tag(data, b'HCA\0'):
			header_size = data[6] << 8 | data[7]
			if header_size > size or header_size < 14:
				return
			for i in range(8, header_size - 6):
				if _is_hca_tag(data + i, b'comp') or _is_hca_tag(data + i, b'dec\0'):
					self._block_size = data[i + 4] << 8 | data[i + 5]
				elif _is_hca_tag(data + i, b'ciph'):
					self._cipher_type = data[i + 4] << 8 | data[i + 5]
					# the header we output is plain
					data[i + 4] = 0
					data[i + 5] = 0
			self.write_check_sum(data, header_size)
			return
		if self._cipher_type == 1:
			_ciph_table = default_audio_ciph_table
		elif self._cipher_type == 56:
			_ciph_table = self._ciph_table
		else:
			return
		block_size = self._block_size if self._block_size else size
		with nogil:
			pos = 0
			while pos + block_size <= size:
				for i in range(pos, pos + block_size):
					data[i] = _ciph_table[data[i]]
				self.write_check_sum(data + pos, block_size)
				pos += block_size

	@wraparound(False)
	@boundscheck(False)
	cdef void write_check_sum(self, uint8_t* data, uint64_t size) noexcept nogil:
		cdef uint64_t i
		cdef uint16_t sum = 0
		for i in range(size - 2):
			sum = (sum << 8) ^ hca_check_sum_table[(sum >> 8) ^ data[i]]
		data[size - 2] = sum >> 8
		data[size - 1] = sum & 0xff


@wraparound(False)
@boundscheck(False)
cdef void init56_create_table(uint8_t* table, uint8_t key) noexcept nogil:
	cdef uint8_t mul = (key & 1) << 3 | 5
	cdef uint8_t add = key & 0xE | 1
	cdef uint64_t i
	key >>= 4
	for i in range(0x10):
		key = (key * mul + add) & 0xF
		table[i] = key


cdef class AesCrypter:
	"""
	AES-128-CTR video encryption used by HSR since 4.4, iv = nonce(8) + frame time(4) + zero(4)
	"""
	cdef bytes _key
	cdef bytes _nonce

	def __init__(self, bytes key, uint64_t nonce):
		self._key = key
		self._nonce = nonce.to_bytes(8, 'big')

	def decrypt_video(self, SimpleBuffer chunk_array):
		from Crypto.Cipher import AES
		cdef Py_ssize_t dataOffset = 0x40
		if chunk_array.size <= dataOffset:
			return
		data = memoryview(chunk_array)[dataOffset:]
		cipher = AES.new(self._key, AES.MODE_CTR, nonce=b'',
						 initial_value=self._nonce + chunk_array.frame_time.to_bytes(4, 'big') + b'\0\0\0\0')
		cipher.decrypt(data, output=data)
