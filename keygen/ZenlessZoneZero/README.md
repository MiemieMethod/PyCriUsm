Keys are generated from ``FileCfg/VideoUSMEncTemplateTb.json``.

Put the file of the new version at ``<version>/VideoUSMEncTemplateTb.json``, change VERSION and run ``python get_keys.py``. Only the file of this version is needed.

Video names in keys.json keep their folders like ``Hollow/HollowToBattleBangboo``, since videos in different folders may have the same name.

Some encrypted videos (like ``Gacha/Gacha_Claret``) have key 0 in the file, they are skipped and cracked when extracting.
