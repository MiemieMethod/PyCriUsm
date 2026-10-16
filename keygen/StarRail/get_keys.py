from hashlib import pbkdf2_hmac

from dataclasses_json import dataclass_json, LetterCase
from dataclasses import dataclass
import json
from pathlib import Path


@dataclass_json(letter_case=LetterCase.PASCAL)
@dataclass
class VideoConfigRow:
	VideoID: int = 0
	VideoPath: str = ''
	IsPlayerInvolved: bool = False
	CaptionPath: str = ''

@dataclass_json(letter_case=LetterCase.PASCAL)
@dataclass
class VideoEncryptionConfigRow:
	VideoID: int = 0
	Encryption: bool = False
	EncryptionMethod: str = 'ADX'


# obfuscated field names of the extra key seeds in GetVideoVersionKeyScRsp since 4.4, they may change every version
AUDIO_KEY_FIELD = 'IBDIMHPOPOJ'
AES_KEY_FIELD = 'BOGECOHIINH'


def _get_hsr_decrypt_key_0_7(video_name: str, version_key: int):
	video_name = video_name.encode()
	name_hash = 0
	for i in video_name:
		name_hash = (name_hash * 11 + i) & 0xffffffffffffffff
	return ((version_key + name_hash) & 0xffffffffffffffff) % 72043514036987937

def _get_hsr_name_hash_2_2(video_name: str, game_version: int, name_stripped: int):
	video_name = video_name[name_stripped:]
	rot = game_version % len(video_name)
	video_name = video_name[rot:] + video_name[:rot]
	video_name = video_name.encode()
	name_hash = 0
	for i in video_name:
		name_hash = (name_hash * 7 + i) & 0xffffffffffffffff
	return name_hash

def _get_hsr_decrypt_key_2_2(video_name: str, version_key: int, name_stripped: int):
	game_version, version_key = divmod(version_key, 10000000000000000)
	name_hash = _get_hsr_name_hash_2_2(video_name, game_version, name_stripped)
	return ((version_key + name_hash) & 0xffffffffffffffff) % 72043514036987937

def _get_hsr_decrypt_key(video_name: str, version_key: int, name_stripped: int):
	if version_key < 72043514036987937:
		return _get_hsr_decrypt_key_0_7(video_name, version_key)
	else:
		return _get_hsr_decrypt_key_2_2(video_name, version_key, name_stripped)

def _get_hsr_hca_key_4_4(video_name: str, version_key: int, audio_key: int):
	game_version = version_key // 10000000000000000
	name_hash = _get_hsr_name_hash_2_2(video_name, game_version, 0)
	return ((name_hash + audio_key - game_version * 10000000000000000) & 0xffffffffffffffff) % 72043514036987937

def _get_hsr_aes_key_4_4(video_name: str, version_key: int, aes_key: int):
	game_version, version_key = divmod(version_key, 10000000000000000)
	password = f'VideoAES_Seed_7svc05j3vc|{game_version:02d}|{version_key:016d}|{aes_key:020d}'
	salt = 'VideoAES_Salt_yzt89k1huq|' + video_name
	return pbkdf2_hmac('sha1', password.encode(), salt.encode(), 10000, 16).hex()

def get_keys(update_for: str):
	root = Path(__file__).parent
	loaded_key = set()
	keys_path = Path(__file__).parent.parent.parent / 'PyCriUsm' / 'keys.json'
	if not keys_path.exists():
		print(f'keys.json not found at {keys_path}')
		return
	with open(keys_path, 'r', encoding='utf-8') as f:
		keys_data = json.load(f)
	all_keys = keys_data["StarRail"]["KeyMap"]
	if update_for not in all_keys:
		all_keys[update_for] = {}

	sub_root = root / update_for
	if sub_root.is_file() or sub_root.name[0].isdigit() is False:
		return
	with open(sub_root / 'GetVideoVersionKeyScRsp.json') as f:
		data = json.load(f)
	tmp_keys = {}
	which_is_video = 'video_key_info_list'
	which_is_cg = 'activity_video_key_info_list'
	for group_key in (which_is_video, which_is_cg):
		tmp_keys[group_key] = {}
		for item in data.get(group_key, ()):
			if item['id'] not in tmp_keys[group_key]:
				tmp_keys[group_key][item['id']] = (int(item['video_key']), int(item.get(AUDIO_KEY_FIELD, 0)),
												   int(item.get(AES_KEY_FIELD, 0)))

	keys = {}
	with open(sub_root / 'VideoConfig.json') as f:
		data = json.load(f)
	with open(sub_root / 'VideoConfigLD.json') as f:
		data.extend(json.load(f))
	with open(sub_root / 'LoopCGConfig.json') as f:
		data.extend(json.load(f))
	with open(sub_root / 'LoopCGConfigLD.json') as f:
		data.extend(json.load(f))
	with open(sub_root / 'VideoEncryptionConfig.json') as f:
		is_encryption_data = json.load(f)
	with open(sub_root / 'LoopCGEncryptionConfig.json') as f:
		is_encryption_data.extend(json.load(f))
	if isinstance(data, dict):
		data = data.values()
	if isinstance(is_encryption_data, list):
		new_is_encryption_data = {}
		for j in is_encryption_data:
			j = VideoEncryptionConfigRow.from_dict(j)
			new_is_encryption_data[str(j.VideoID)] = j
		is_encryption_data = new_is_encryption_data
	for i in data:
		i = VideoConfigRow.from_dict(i)
		if i.VideoID in loaded_key:
			continue
		loaded_key.add(i.VideoID)
		encryption = is_encryption_data.get(str(i.VideoID), VideoEncryptionConfigRow())
		if encryption.Encryption is False:
			continue
		version_key = None
		name_stripped = 0
		if which_is_video is not None and i.VideoID in tmp_keys[which_is_video]:
			version_key = tmp_keys[which_is_video][i.VideoID]
			name_stripped = 0
		if which_is_cg is not None and i.VideoID in tmp_keys[which_is_cg]:
			version_key = tmp_keys[which_is_cg][i.VideoID]
			name_stripped = 7
		if version_key is None:
			Warning(f'Could not find {i.VideoPath} key')
			continue
		version_key, audio_key, aes_key = version_key
		if encryption.EncryptionMethod == 'AES_HCA' and not aes_key:
			print(f'Could not find {i.VideoPath} AES key seed, skip it')
			continue
		names = i.VideoPath.removesuffix('.usm')
		if i.IsPlayerInvolved:
			names = (names + '_f', names + '_m')
		else:
			names = (names,)
		for name in names:
			if name in keys:
				continue
			if 'AvatarShowLoop' in name: # thanks to hiro
				name_stripped = 0
			if encryption.EncryptionMethod == 'AES_HCA': # thanks to hiro
				# [AES video key, HCA audio key]
				key = [_get_hsr_aes_key_4_4(name, version_key, aes_key),
					   _get_hsr_hca_key_4_4(name, version_key, audio_key)]
			else:
				key = _get_hsr_decrypt_key(name, version_key, name_stripped)
			keys[name] = key

	for name in keys:
		key = keys[name]
		updated = False
		for group_key, group in all_keys.items():
			if name in group:
				group[name] = key
				updated = True
		if not updated:
			all_keys[update_for][name] = key
	return 1, all_keys


if __name__ == '__main__':
	VERSION = "4.6"
	result, all_keys = get_keys(VERSION)
	keys_path = Path(__file__).parent.parent.parent / 'PyCriUsm' / 'keys.json'
	with open(keys_path, 'r', encoding='utf-8') as f:
		keys = json.load(f)
	keys["StarRail"] = {'Encrytion': result, 'KeyMap': all_keys}
	with open(keys_path, 'w', encoding='utf-8') as f:
		json.dump(keys, f, ensure_ascii=False, indent=4)